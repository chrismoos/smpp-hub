use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    Action, Body, CommandStatus, Config, MessageBody, Pdu, PduReader, PduWriter, Role, Session,
    State,
};

const COMMAND_CAPACITY: usize = 16;
const EVENT_CAPACITY: usize = 32;
const DRIVER_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_BIND_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub io_timeout: Duration,
    pub bind_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            io_timeout: DEFAULT_IO_TIMEOUT,
            bind_timeout: DEFAULT_BIND_TIMEOUT,
            shutdown_timeout: DEFAULT_IO_TIMEOUT,
        }
    }
}

type Reply = oneshot::Sender<io::Result<u32>>;

enum Command {
    Message(Box<MessageBody>, Reply),
    Send(Pdu, Reply),
    Accept(u32, String, Reply),
    Reject(u32, CommandStatus, Reply),
    Shutdown(Reply),
}

pub struct Connection {
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<Action>,
    result: Option<oneshot::Receiver<io::Result<()>>>,
    state: watch::Receiver<State>,
    outstanding: Arc<AtomicUsize>,
    role: Role,
}

impl Connection {
    /// Dropping the handle starts a bounded unbind. Call shutdown to wait for it.
    pub fn new<S>(stream: S, config: Config, options: ConnectionOptions) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (events, event_receiver) = mpsc::channel(EVENT_CAPACITY);
        let (finished, result_receiver) = oneshot::channel();
        let (state, state_receiver) = watch::channel(State::Open);
        let outstanding = Arc::new(AtomicUsize::new(0));
        let role = config.role;
        let count = outstanding.clone();
        tokio::spawn(async move {
            let (read, write) = tokio::io::split(stream);
            let driver = Driver {
                session: Session::new(config, Instant::now()),
                reader: PduReader::new(read),
                writer: PduWriter::new(write),
                options,
                state: state.clone(),
                outstanding: count,
                pending_bind: None,
            };
            let result = driver.run(receiver, &events).await;
            state.send_replace(State::Closed);
            let _ = finished.send(result);
        });
        Self {
            commands,
            events: event_receiver,
            result: Some(result_receiver),
            state: state_receiver,
            outstanding,
            role,
        }
    }

    pub fn state(&self) -> State {
        *self.state.borrow()
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Cancellation preserves queued events and never interrupts a protocol write.
    pub async fn next(&mut self) -> io::Result<Option<Action>> {
        if let Some(event) = self.events.recv().await {
            return Ok(Some(event));
        }
        if let Some(result) = self.result.as_mut() {
            let result = result.await.map_err(|_| closed())?;
            self.result = None;
            result?;
        }
        Ok(None)
    }

    /// Canceling this future does not retract a queued submission. Its outcome may be unknown.
    pub async fn send_message(&self, body: MessageBody) -> io::Result<u32> {
        let (reply, result) = oneshot::channel();
        self.request(Command::Message(Box::new(body), reply), result)
            .await
    }

    pub async fn send(&self, pdu: Pdu) -> io::Result<()> {
        if !matches!(
            pdu.body,
            Body::SubmitSmResp { .. } | Body::DeliverSmResp { .. } | Body::GenericNack
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "use connection methods for SMPP requests and bind responses",
            ));
        }
        let (reply, result) = oneshot::channel();
        self.request(Command::Send(pdu, reply), result)
            .await
            .map(|_| ())
    }

    pub async fn accept_bind(&self, sequence: u32, system_id: String) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.request(Command::Accept(sequence, system_id, reply), result)
            .await
            .map(|_| ())
    }

    pub async fn reject_bind(&self, sequence: u32, status: CommandStatus) -> io::Result<()> {
        if status == CommandStatus::Ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SMPP bind rejection requires an error status",
            ));
        }
        let (reply, result) = oneshot::channel();
        self.request(Command::Reject(sequence, status, reply), result)
            .await
            .map(|_| ())
    }

    pub fn respond_to_message(
        &self,
        sequence: u32,
        status: CommandStatus,
        message_id: String,
    ) -> Pdu {
        crate::session::message_response(self.role, sequence, status, message_id)
    }

    pub async fn shutdown(&self) -> io::Result<()> {
        if self.state() == State::Closed {
            return Ok(());
        }
        let (reply, result) = oneshot::channel();
        self.request(Command::Shutdown(reply), result)
            .await
            .map(|_| ())
    }

    async fn request(
        &self,
        command: Command,
        result: oneshot::Receiver<io::Result<u32>>,
    ) -> io::Result<u32> {
        self.commands.send(command).await.map_err(|_| closed())?;
        result.await.map_err(|_| closed())?
    }
}

struct Driver<R, W> {
    session: Session,
    reader: PduReader<R>,
    writer: PduWriter<W>,
    options: ConnectionOptions,
    state: watch::Sender<State>,
    outstanding: Arc<AtomicUsize>,
    pending_bind: Option<(u32, String)>,
}

impl<R, W> Driver<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn update(&self) {
        self.state.send_replace(self.session.state());
        self.outstanding
            .store(self.session.outstanding(), Ordering::Relaxed);
    }

    async fn write(&mut self, pdu: &Pdu) -> io::Result<()> {
        tokio::time::timeout(self.options.io_timeout, self.writer.send(pdu)).await?
    }

    async fn run(
        mut self,
        mut commands: mpsc::Receiver<Command>,
        events: &mpsc::Sender<Action>,
    ) -> io::Result<()> {
        if self.session.role() == Role::Esme {
            let bind = self.session.bind(Instant::now()).map_err(protocol_error)?;
            self.write(&bind).await?;
        }
        self.update();
        let bind_deadline = tokio::time::Instant::now() + self.options.bind_timeout;
        let mut ticker = tokio::time::interval(DRIVER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let actions = tokio::select! {
                _ = events.closed() => return self.unbind(events).await,
                _ = tokio::time::sleep_until(bind_deadline), if matches!(self.session.state(), State::Open | State::BindPending) => return Err(io::Error::new(io::ErrorKind::TimedOut, "SMPP bind timed out")),
                command = commands.recv() => {
                    let Some(command) = command else { return self.unbind(events).await; };
                    if let Command::Shutdown(reply) = command {
                        let result = self.unbind(events).await;
                        let response = match &result {
                            Ok(()) => Ok(0),
                            Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
                        };
                        let _ = reply.send(response);
                        return result;
                    }
                    self.command(command).await?;
                    Vec::new()
                }
                pdu = self.reader.next(), if self.pending_bind.is_none() => match pdu? {
                    Some(pdu) => self.session.handle(pdu, Instant::now()),
                    None => return Ok(()),
                },
                _ = ticker.tick() => self.session.tick(Instant::now()),
            };
            self.update();
            for action in actions {
                match action {
                    Action::Send(pdu) => self.write(&pdu).await?,
                    Action::Closed => return Ok(()),
                    event => {
                        if let Action::BindRequest {
                            sequence,
                            system_id,
                            ..
                        } = &event
                        {
                            self.pending_bind = Some((*sequence, system_id.clone()));
                        }
                        if tokio::time::timeout(self.options.io_timeout, events.send(event))
                            .await?
                            .is_err()
                        {
                            return self.unbind(events).await;
                        }
                    }
                }
            }
        }
    }

    async fn command(&mut self, command: Command) -> io::Result<()> {
        let (pdu, reply) = match command {
            Command::Message(body, reply) => match self.session.send_message(*body, Instant::now())
            {
                Ok((_, pdu)) => (pdu, reply),
                Err(error) => {
                    let _ = reply.send(Err(protocol_error(error)));
                    return Ok(());
                }
            },
            Command::Send(pdu, reply) => (pdu, reply),
            Command::Accept(sequence, id, reply) => {
                if self.pending_bind.as_ref() != Some(&(sequence, id.clone())) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "no matching SMPP bind request",
                    )));
                    return Ok(());
                }
                self.pending_bind = None;
                (
                    self.session.accept_bind(sequence, id, Instant::now()),
                    reply,
                )
            }
            Command::Reject(sequence, status, reply) => {
                if !self
                    .pending_bind
                    .as_ref()
                    .is_some_and(|(pending, _)| *pending == sequence)
                {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "no matching SMPP bind request",
                    )));
                    return Ok(());
                }
                self.pending_bind = None;
                (self.session.reject_bind(sequence, status), reply)
            }
            Command::Shutdown(_) => unreachable!(),
        };
        let sequence = pdu.sequence_number;
        if let Err(error) = self.write(&pdu).await {
            let _ = reply.send(Err(io::Error::new(error.kind(), error.to_string())));
            return Err(error);
        }
        self.update();
        let _ = reply.send(Ok(sequence));
        if self.session.state() == State::Closed {
            return Err(closed());
        }
        Ok(())
    }

    async fn unbind(&mut self, events: &mpsc::Sender<Action>) -> io::Result<()> {
        if self.session.state() != State::Bound {
            return Ok(());
        }
        let timeout = self.options.shutdown_timeout;
        tokio::time::timeout(timeout, async {
            let pdu = self
                .session
                .unbind(Instant::now())
                .map_err(protocol_error)?;
            self.update();
            self.write(&pdu).await?;
            while let Some(pdu) = self.reader.next().await? {
                for action in self.session.handle(pdu, Instant::now()) {
                    match action {
                        Action::Send(pdu) => self.write(&pdu).await?,
                        Action::Closed => return Ok(()),
                        event if !events.is_closed() => {
                            let _ = tokio::time::timeout(timeout, events.send(event)).await?;
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        })
        .await?
    }
}

fn protocol_error(error: crate::Error) -> io::Error {
    let kind = if error == crate::Error::WindowFull {
        io::ErrorKind::WouldBlock
    } else {
        io::ErrorKind::InvalidData
    };
    io::Error::new(kind, error)
}
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "SMPP connection closed")
}

#[cfg(test)]
mod tests;
