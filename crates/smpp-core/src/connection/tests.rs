use super::*;
use crate::{Address, Bind, BindKind};
use tokio::io::{AsyncWriteExt, DuplexStream, duplex};
use tokio::time::timeout;

const TEST_DEADLINE: Duration = Duration::from_secs(2);
const SHORT_DEADLINE: Duration = Duration::from_millis(30);
const STREAM_CAPACITY: usize = 4096;

fn options() -> ConnectionOptions {
    ConnectionOptions {
        io_timeout: SHORT_DEADLINE,
        bind_timeout: SHORT_DEADLINE,
        shutdown_timeout: SHORT_DEADLINE,
    }
}

async fn event(connection: &mut Connection) -> Action {
    timeout(TEST_DEADLINE, connection.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn pair() -> (Connection, Connection) {
    let (left, right) = duplex(STREAM_CAPACITY);
    let client = Connection::new(
        left,
        Config::esme("client", "secret"),
        ConnectionOptions::default(),
    );
    let mut server = Connection::new(right, Config::smsc("server"), ConnectionOptions::default());
    let Action::BindRequest {
        sequence,
        system_id,
        password,
    } = event(&mut server).await
    else {
        panic!("bind request");
    };
    assert_eq!(password, "secret");
    server.accept_bind(sequence, system_id).await.unwrap();
    let mut client = client;
    assert!(matches!(event(&mut client).await, Action::Bound { .. }));
    (client, server)
}

#[tokio::test]
async fn client_and_server_exchange_messages_and_response_ids() {
    let (mut client, mut server) = pair().await;
    let body = MessageBody {
        source: Address::unknown("1000"),
        destination: Address::unknown("2000"),
        text: b"hello".to_vec(),
        ..Default::default()
    };
    let sent = client.send_message(body.clone()).await.unwrap();
    let Action::Message {
        sequence,
        body: received,
    } = event(&mut server).await
    else {
        panic!("message");
    };
    assert_eq!(*received, body);
    server
        .send(server.respond_to_message(sequence, CommandStatus::Ok, "stored-id".into()))
        .await
        .unwrap();
    assert_eq!(
        event(&mut client).await,
        Action::MessageAck {
            sequence: sent,
            status: CommandStatus::Ok,
            message_id: "stored-id".into()
        }
    );
    client.shutdown().await.unwrap();
    assert!(server.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_credentials_can_be_rejected_before_messages() {
    let (left, right) = duplex(STREAM_CAPACITY);
    let mut client = Connection::new(
        left,
        Config::esme("client", "wrong"),
        ConnectionOptions::default(),
    );
    let mut server = Connection::new(right, Config::smsc("server"), ConnectionOptions::default());
    let Action::BindRequest { sequence, .. } = event(&mut server).await else {
        panic!("bind");
    };
    server
        .reject_bind(sequence, CommandStatus::BindFailed)
        .await
        .unwrap();
    assert!(client.next().await.unwrap().is_none());
    assert!(client.send_message(MessageBody::default()).await.is_err());
}

#[tokio::test]
async fn silent_peer_hits_bind_deadline_without_application_polling() {
    let (_peer, stream) = duplex(STREAM_CAPACITY);
    let mut server = Connection::new(stream, Config::smsc("server"), options());
    assert_eq!(
        timeout(TEST_DEADLINE, server.next())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(server.state(), State::Closed);
}

#[tokio::test]
async fn backpressured_bind_write_has_a_deadline() {
    let (_peer, stream) = duplex(1);
    let mut client = Connection::new(stream, Config::esme("client", "secret"), options());
    assert_eq!(
        timeout(TEST_DEADLINE, client.next())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
}

async fn raw_peer() -> (Connection, DuplexStream) {
    let (mut peer, stream) = duplex(STREAM_CAPACITY);
    let mut server = Connection::new(stream, Config::smsc("server"), options());
    let bind = Pdu::request(
        1,
        Body::Bind(Bind {
            kind: BindKind::Transceiver,
            system_id: "client".into(),
            password: "secret".into(),
            system_type: String::new(),
            interface_version: crate::pdu::INTERFACE_VERSION_34,
            address_range: Address::default(),
        }),
    );
    peer.write_all(&crate::encode(&bind).unwrap())
        .await
        .unwrap();
    let Action::BindRequest {
        sequence,
        system_id,
        ..
    } = event(&mut server).await
    else {
        panic!("bind");
    };
    server.accept_bind(sequence, system_id).await.unwrap();
    let reply = PduReader::new(&mut peer).next().await.unwrap().unwrap();
    assert_eq!(reply.status, CommandStatus::Ok);
    (server, peer)
}

#[tokio::test]
async fn graceful_shutdown_waits_for_unbind_response() {
    let (server, mut peer) = raw_peer().await;
    let shutdown = tokio::spawn(async move { server.shutdown().await });
    let request = PduReader::new(&mut peer).next().await.unwrap().unwrap();
    assert_eq!(request.body, Body::Unbind);
    assert!(!shutdown.is_finished());
    PduWriter::new(&mut peer)
        .send(&Pdu::response(
            request.sequence_number,
            CommandStatus::Ok,
            Body::UnbindResp,
        ))
        .await
        .unwrap();
    shutdown.await.unwrap().unwrap();
}

#[tokio::test]
async fn graceful_shutdown_times_out_for_unresponsive_peer() {
    let (server, _peer) = raw_peer().await;
    assert_eq!(
        timeout(TEST_DEADLINE, server.shutdown())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn dropping_a_handle_sends_unbind() {
    let (server, mut peer) = raw_peer().await;
    drop(server);
    let request = timeout(TEST_DEADLINE, PduReader::new(&mut peer).next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(request.body, Body::Unbind);
}

#[tokio::test]
async fn canceled_event_wait_preserves_partial_frames() {
    let (mut server, mut peer) = raw_peer().await;
    let message = Pdu::request(2, Body::SubmitSm(Box::default()));
    let bytes = crate::encode(&message).unwrap();
    let split = bytes.len() / 2;
    peer.write_all(&bytes[..split]).await.unwrap();
    assert!(timeout(SHORT_DEADLINE, server.next()).await.is_err());
    peer.write_all(&bytes[split..]).await.unwrap();
    assert!(matches!(
        event(&mut server).await,
        Action::Message { sequence: 2, .. }
    ));
}

#[tokio::test]
async fn full_window_does_not_close_connection() {
    let (left, right) = duplex(STREAM_CAPACITY);
    let mut config = Config::esme("client", "secret");
    config.window = 1;
    let mut client = Connection::new(left, config, ConnectionOptions::default());
    let mut server = Connection::new(right, Config::smsc("server"), ConnectionOptions::default());
    let Action::BindRequest {
        sequence,
        system_id,
        ..
    } = event(&mut server).await
    else {
        panic!("bind");
    };
    server.accept_bind(sequence, system_id).await.unwrap();
    event(&mut client).await;
    client.send_message(MessageBody::default()).await.unwrap();
    assert_eq!(
        client
            .send_message(MessageBody::default())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(client.state(), State::Bound);
}

#[tokio::test]
async fn bind_acceptance_requires_the_actual_pending_request() {
    let (mut server, _peer) = raw_peer().await;
    assert_eq!(
        server
            .accept_bind(1, "client".into())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        server
            .send(Pdu::request(1, Body::Unbind))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(server.state(), State::Bound);
    assert!(timeout(SHORT_DEADLINE, server.next()).await.is_err());
}

#[tokio::test]
async fn timers_run_without_waiting_for_application_events() {
    let (left, right) = duplex(STREAM_CAPACITY);
    let mut config = Config::esme("client", "secret");
    config.enquire_interval = SHORT_DEADLINE;
    config.request_timeout = SHORT_DEADLINE;
    let mut client = Connection::new(left, config, ConnectionOptions::default());
    let mut server = Connection::new(right, Config::smsc("server"), ConnectionOptions::default());
    let Action::BindRequest {
        sequence,
        system_id,
        ..
    } = event(&mut server).await
    else {
        panic!("bind");
    };
    server.accept_bind(sequence, system_id).await.unwrap();
    event(&mut client).await;
    let sequence = client.send_message(MessageBody::default()).await.unwrap();
    assert_eq!(
        event(&mut client).await,
        Action::RequestTimeout { sequence }
    );
    tokio::time::sleep(DRIVER_INTERVAL * 3).await;
    assert_eq!(client.state(), State::Bound);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_backpressure_disconnects_and_preserves_terminal_error() {
    let (mut server, mut peer) = raw_peer().await;
    for sequence in 1..=EVENT_CAPACITY + 1 {
        PduWriter::new(&mut peer)
            .send(&Pdu::request(
                sequence as u32,
                Body::SubmitSm(Box::default()),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(SHORT_DEADLINE * 3).await;
    let mut received = 0;
    loop {
        match timeout(TEST_DEADLINE, server.next()).await.unwrap() {
            Ok(Some(Action::Message { .. })) => received += 1,
            Err(error) => {
                assert_eq!(error.kind(), io::ErrorKind::TimedOut);
                break;
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(received, EVENT_CAPACITY);
}

#[tokio::test]
async fn shutdown_preserves_message_acknowledgments_for_the_application() {
    let (server, mut peer) = raw_peer().await;
    let sequence = server.send_message(MessageBody::default()).await.unwrap();
    let message = PduReader::new(&mut peer).next().await.unwrap().unwrap();
    assert_eq!(message.sequence_number, sequence);
    let task = tokio::spawn(async move {
        let result = server.shutdown().await;
        (server, result)
    });
    let unbind = PduReader::new(&mut peer).next().await.unwrap().unwrap();
    let mut writer = PduWriter::new(&mut peer);
    writer
        .send(&Pdu::response(
            sequence,
            CommandStatus::Ok,
            Body::DeliverSmResp {
                message_id: String::new(),
            },
        ))
        .await
        .unwrap();
    writer
        .send(&Pdu::response(
            unbind.sequence_number,
            CommandStatus::Ok,
            Body::UnbindResp,
        ))
        .await
        .unwrap();
    let (mut server, result) = task.await.unwrap();
    result.unwrap();
    assert_eq!(
        event(&mut server).await,
        Action::MessageAck {
            sequence,
            status: CommandStatus::Ok,
            message_id: String::new()
        }
    );
    assert!(server.next().await.unwrap().is_none());
}
