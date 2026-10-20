use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

const STOP_INTERVAL: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(address).await?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, peer) = self.listener.accept().await?;
        stream.set_nodelay(true)?;
        Ok((stream, peer))
    }
}

pub async fn connect(address: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(address)).await??;
    stream.set_nodelay(true)?;
    Ok(stream)
}

#[derive(Clone, Copy, Debug)]
pub enum Endpoint {
    Client(SocketAddr),
    Server(SocketAddr),
}

pub struct Connector {
    endpoint: Endpoint,
    listener: Option<Server>,
    attempted: bool,
    pub connect_timeout: Duration,
    pub reconnect_delay: Duration,
}

impl Connector {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            listener: None,
            attempted: false,
            connect_timeout: CONNECT_TIMEOUT,
            reconnect_delay: RECONNECT_DELAY,
        }
    }

    /// Returns None when stop requests shutdown, including during backoff or accept.
    pub async fn next(&mut self, stop: impl Fn() -> bool) -> io::Result<Option<TcpStream>> {
        if stop() {
            return Ok(None);
        }
        let cancel = async {
            loop {
                tokio::time::sleep(STOP_INTERVAL).await;
                if stop() {
                    break;
                }
            }
        };
        tokio::select! {
            _ = cancel => Ok(None),
            result = self.open() => result.map(Some),
        }
    }

    async fn open(&mut self) -> io::Result<TcpStream> {
        if self.attempted {
            tokio::time::sleep(self.reconnect_delay).await;
        }
        self.attempted = true;
        match self.endpoint {
            Endpoint::Client(address) => connect(address, self.connect_timeout).await,
            Endpoint::Server(address) => {
                if self.listener.is_none() {
                    self.listener = Some(Server::bind(address).await?);
                }
                self.listener
                    .as_ref()
                    .expect("listener initialized above")
                    .accept()
                    .await
                    .map(|(stream, _)| stream)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn accepted_and_connected_tcp_streams_disable_nagle() {
        let server = Server::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let (client, accepted) = tokio::join!(
            connect(server.local_addr().unwrap(), TEST_TIMEOUT),
            server.accept()
        );
        assert!(client.unwrap().nodelay().unwrap());
        assert!(accepted.unwrap().0.nodelay().unwrap());
    }

    #[tokio::test]
    async fn connector_cancels_accept_on_stop() {
        let stop = AtomicBool::new(false);
        let mut connector = Connector::new(Endpoint::Server("127.0.0.1:0".parse().unwrap()));
        let request = async {
            connector
                .next(|| stop.load(Ordering::Relaxed))
                .await
                .unwrap()
        };
        let cancel = async {
            tokio::time::sleep(STOP_INTERVAL).await;
            stop.store(true, Ordering::Relaxed);
        };
        let (stream, ()) =
            tokio::time::timeout(TEST_TIMEOUT, async { tokio::join!(request, cancel) })
                .await
                .unwrap();
        assert!(stream.is_none());
        assert!(connector.next(|| true).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn server_connector_retains_listener_between_connections() {
        let mut connector = Connector::new(Endpoint::Server("127.0.0.1:0".parse().unwrap()));
        connector.reconnect_delay = Duration::ZERO;
        assert!(
            tokio::time::timeout(STOP_INTERVAL, connector.next(|| false))
                .await
                .is_err()
        );
        let address = connector.listener.as_ref().unwrap().local_addr().unwrap();
        for _ in 0..2 {
            let (client, accepted) =
                tokio::join!(connect(address, TEST_TIMEOUT), connector.next(|| false));
            assert!(client.unwrap().nodelay().unwrap());
            assert!(accepted.unwrap().unwrap().nodelay().unwrap());
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn reconnect_backoff_is_interruptible() {
        let listener = Server::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut connector = Connector::new(Endpoint::Client(address));
        assert!(connector.next(|| false).await.is_err());
        let stop = AtomicBool::new(false);
        let retry = connector.next(|| stop.load(Ordering::Relaxed));
        let cancel = async {
            tokio::time::sleep(STOP_INTERVAL).await;
            stop.store(true, Ordering::Relaxed);
        };
        let (retry, ()) =
            tokio::time::timeout(STOP_INTERVAL * 4, async { tokio::join!(retry, cancel) })
                .await
                .unwrap();
        assert!(retry.unwrap().is_none());
    }
}
