use std::io;

use smpp_core::tcp::Server;
use smpp_core::{Action, CommandStatus, Config, Connection, ConnectionOptions, MessageBody};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;

const MAX_STORED_MESSAGES: usize = 100;
const DEFAULT_ADDRESS: &str = "127.0.0.1:2775";
const EXAMPLE_SYSTEM_ID: &str = "example";
const EXAMPLE_PASSWORD: &str = "password";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDRESS.into())
        .parse()?;
    let server = Server::bind(address).await?;
    let (stop, _) = watch::channel(false);
    let mut tasks = JoinSet::new();
    println!(
        "Echo server listening on {}. Press Ctrl+C to stop.",
        server.local_addr()?
    );
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            result = server.accept() => {
                let (stream, peer) = result?;
                let stopping = stop.subscribe();
                tasks.spawn(async move {
                    if let Err(error) = serve(stream, stopping).await {
                        eprintln!("Connection from {peer}: {error}");
                    }
                });
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => { result?; }
        }
    }
    stop.send_replace(true);
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    Ok(())
}

async fn serve(stream: TcpStream, mut stop: watch::Receiver<bool>) -> io::Result<()> {
    let mut connection = Connection::new(
        stream,
        Config::smsc("example-server"),
        ConnectionOptions::default(),
    );
    let mut messages: Vec<MessageBody> = Vec::new();
    let result = async {
        loop {
            let event = tokio::select! {
                _ = stop.changed() => return Ok(()),
                event = connection.next() => event?,
            };
            match event {
                Some(Action::BindRequest {
                    sequence,
                    system_id,
                    password,
                }) => {
                    if system_id != EXAMPLE_SYSTEM_ID || password != EXAMPLE_PASSWORD {
                        connection
                            .reject_bind(sequence, CommandStatus::BindFailed)
                            .await?;
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "bind refused",
                        ));
                    }
                    connection.accept_bind(sequence, system_id).await?;
                }
                Some(Action::Message { sequence, body }) => {
                    if messages.len() == MAX_STORED_MESSAGES {
                        connection
                            .send(connection.respond_to_message(
                                sequence,
                                CommandStatus::MessageQueueFull,
                                String::new(),
                            ))
                            .await?;
                        continue;
                    }
                    messages.push(*body);
                    let message = messages.last().expect("message was just pushed");
                    let id = messages.len().to_string();
                    println!(
                        "Accepted message {id} from {} to {}",
                        message.source.digits, message.destination.digits
                    );
                    connection
                        .send(connection.respond_to_message(sequence, CommandStatus::Ok, id))
                        .await?;
                    let mut echo = message.clone();
                    std::mem::swap(&mut echo.source, &mut echo.destination);
                    connection.send_message(echo).await?;
                }
                Some(Action::MessageAck { status, .. }) => println!("Echo response: {status:?}"),
                Some(Action::RequestTimeout { .. }) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "echo outcome unknown",
                    ));
                }
                None => return Ok(()),
                _ => {}
            }
        }
    }
    .await;
    let shutdown = connection.shutdown().await;
    while let Ok(Some(event)) = connection.next().await {
        if let Action::MessageAck { status, .. } = event {
            println!("Echo response: {status:?}");
        }
    }
    result.and(shutdown)
}
