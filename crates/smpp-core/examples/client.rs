use std::io;

use smpp_core::tcp;
use smpp_core::{
    Action, Address, CommandStatus, Config, Connection, ConnectionOptions, MessageBody,
};

const DEFAULT_ADDRESS: &str = "127.0.0.1:2775";
const EXAMPLE_SYSTEM_ID: &str = "example";
const EXAMPLE_PASSWORD: &str = "password";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let address = args
        .next()
        .unwrap_or_else(|| DEFAULT_ADDRESS.into())
        .parse()?;
    let text = args.next().unwrap_or_else(|| "Hello from smpp-core".into());
    let options = ConnectionOptions::default();
    let stream = tcp::connect(address, options.io_timeout).await?;
    let mut connection = Connection::new(
        stream,
        Config::esme(EXAMPLE_SYSTEM_ID, EXAMPLE_PASSWORD),
        options,
    );
    loop {
        match connection.next().await? {
            Some(Action::Bound { system_id }) => {
                println!("Connected to {system_id}");
                break;
            }
            None => return Err(io::Error::new(io::ErrorKind::NotConnected, "bind failed").into()),
            _ => {}
        }
    }
    let (data_coding, text) = smpp_core::text::encode(&text)?;
    let sequence = connection
        .send_message(MessageBody {
            source: Address::unknown("1000"),
            destination: Address::unknown("2000"),
            data_coding,
            text,
            ..Default::default()
        })
        .await?;
    let mut accepted = false;
    let mut echoed = false;
    while !accepted || !echoed {
        match connection.next().await? {
            Some(Action::MessageAck {
                sequence: reply,
                status,
                message_id,
            }) if reply == sequence => {
                if status != CommandStatus::Ok {
                    return Err(io::Error::other(format!("message refused: {status:?}")).into());
                }
                println!("Server accepted message {message_id}");
                accepted = true;
            }
            Some(Action::Message { sequence, body }) => {
                println!(
                    "Echo from {}: {}",
                    body.source.digits,
                    smpp_core::text::decode(&body)?
                );
                connection
                    .send(connection.respond_to_message(sequence, CommandStatus::Ok, String::new()))
                    .await?;
                echoed = true;
            }
            Some(Action::RequestTimeout { .. }) => {
                return Err(
                    io::Error::new(io::ErrorKind::TimedOut, "message outcome unknown").into(),
                );
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection ended before the echo",
                )
                .into());
            }
            _ => {}
        }
    }
    connection.shutdown().await?;
    println!("Disconnected cleanly");
    Ok(())
}
