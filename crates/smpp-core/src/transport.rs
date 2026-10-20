use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec;
use crate::error::Error;
use crate::pdu::{HEADER_LEN, MAX_PDU_LEN, Pdu};

pub struct PduReader<R> {
    inner: R,
    buf: Vec<u8>,
}

impl<R: AsyncRead + Unpin> PduReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(4096),
        }
    }

    pub async fn next(&mut self) -> io::Result<Option<Pdu>> {
        loop {
            match codec::decode_frame(&self.buf) {
                Ok(Some((pdu, used))) => {
                    self.buf.drain(..used);
                    return Ok(Some(pdu));
                }
                Ok(None) => {}
                Err(err) => return Err(decode_error(err)),
            }

            if self.buf.len() >= 4 {
                let claimed =
                    u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                if claimed < HEADER_LEN as u32 || claimed > MAX_PDU_LEN {
                    return Err(decode_error(Error::InvalidCommandLength(claimed)));
                }
            }

            let mut chunk = [0u8; 4096];
            let read = self.inner.read(&mut chunk).await?;
            if read == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended mid-PDU",
                ));
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }
}

pub struct PduWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> PduWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub async fn send(&mut self, pdu: &Pdu) -> io::Result<()> {
        let bytes = codec::encode(pdu).map_err(decode_error)?;
        self.inner.write_all(&bytes).await?;
        self.inner.flush().await
    }
}

fn decode_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{Address, Body, MessageBody};
    use tokio::io::duplex;

    #[tokio::test]
    async fn pdus_round_trip_over_a_stream() {
        let (client, server) = duplex(256);
        let (client_read, client_write) = tokio::io::split(client);
        let mut writer = PduWriter::new(client_write);
        let mut reader = PduReader::new(server);

        let msg = MessageBody {
            source: Address::unknown("7025550100"),
            destination: Address::unknown("4155550118"),
            text: vec![b'z'; 900],
            bearer_data: Some(vec![1, 2, 3]),
            ..MessageBody::default()
        };
        let sent = Pdu::request(42, Body::SubmitSm(Box::new(msg)));
        let writer_task = tokio::spawn(async move {
            writer.send(&sent).await.unwrap();
            sent
        });

        let received = reader.next().await.unwrap().expect("a PDU");
        let sent = writer_task.await.unwrap();
        assert_eq!(received, sent);
        drop(client_read);
    }

    #[tokio::test]
    async fn a_clean_close_ends_the_stream() {
        let (client, server) = duplex(64);
        drop(client);
        let mut reader = PduReader::new(server);
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_absurd_length_is_refused_without_reading_it() {
        let (mut client, server) = duplex(64);
        client
            .write_all(&(MAX_PDU_LEN + 1).to_be_bytes())
            .await
            .unwrap();
        let mut reader = PduReader::new(server);
        let err = reader.next().await.expect_err("should refuse");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
