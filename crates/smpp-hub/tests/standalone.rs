#![cfg(feature = "standalone")]

use chrono::Utc;
use smpp_core::{
    Action, Address, CommandStatus, Config, Connection, ConnectionOptions, MessageBody,
};
use smpp_hub::{
    Hub,
    model::*,
    standalone::{Peer, Settings, SqliteStore},
    store::{MessageOutcome, NewMessage, Store},
};
use std::{path::PathBuf, sync::Arc, time::Duration};

const TIMEOUT: Duration = Duration::from_secs(5);

fn settings() -> Settings {
    Settings {
        listen: "127.0.0.1:0".parse().unwrap(),
        system_id: "test-hub".into(),
        database: std::env::temp_dir().join(format!("smpp-hub-{}.sqlite", uuid::Uuid::new_v4())),
        quotas: Quotas::default(),
        reserved_numbers: vec!["911".into()],
        peers: [(1, "a", "1000"), (2, "b", "1001")]
            .into_iter()
            .map(|(id, name, number)| Peer {
                site: Site {
                    id: SiteId(uuid::Uuid::from_u128(id)),
                    name: name.into(),
                    smpp_system_id: name.into(),
                    bind_role: BindRole::SiteBindsToHub,
                    messages_per_hour: None,
                },
                password: "secret".into(),
                numbers: vec![number.into()],
                address: None,
            })
            .collect(),
    }
}

fn submission(s: &Settings) -> NewMessage {
    let now = Utc::now();
    NewMessage {
        smpp_pdu: None,
        receipt_for: None,
        origin_site: s.peers[0].site.id,
        destination_site: s.peers[1].site.id,
        from_number: "1000".into(),
        to_number: "1001".into(),
        text: "hello".into(),
        bearer_data: None,
        teleservice: None,
        receipt_requested: true,
        receipt_failure_only: false,
        received_at: now,
        expires_at: now + chrono::Duration::hours(1),
    }
}

fn cleanup(path: PathBuf) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn queue_survives_restart_and_terminal_payload_is_removed() {
    let s = settings();
    let store = SqliteStore::open(s.clone()).await.unwrap();
    let id = store.accept_message(submission(&s)).await.unwrap();
    drop(store);
    let store = SqliteStore::open(s.clone()).await.unwrap();
    let due = store
        .due_messages(s.peers[1].site.id, Utc::now(), 10)
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, id);
    assert_eq!(due[0].text, "hello");
    store
        .complete_message(
            id,
            MessageOutcome {
                state: MessageState::Delivered,
                next_attempt_at: None,
                last_error: None,
            },
        )
        .await
        .unwrap();
    let m = store.message(id).await.unwrap().unwrap();
    assert!(m.text.is_empty());
    assert!(m.smpp_pdu.is_none());
    assert_eq!(m.state, MessageState::Delivered);
    assert!(
        store
            .due_messages(s.peers[1].site.id, Utc::now(), 10)
            .await
            .unwrap()
            .is_empty()
    );
    drop(store);
    cleanup(s.database);
}

#[tokio::test]
async fn duplicate_receipts_are_idempotent_and_remote_ids_are_scoped_to_peer() {
    let s = settings();
    let store = SqliteStore::open(s.clone()).await.unwrap();
    let id = store.accept_message(submission(&s)).await.unwrap();
    store.record_smpp_destination(id, "remote-1").await.unwrap();
    assert!(
        store
            .smpp_receipted_message(s.peers[0].site.id, "remote-1")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .smpp_receipted_message(s.peers[1].site.id, "remote-1")
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    let mut receipt = submission(&s);
    receipt.receipt_for = Some(id);
    let receipt_id = store.accept_message(receipt.clone()).await.unwrap();
    assert_eq!(store.accept_message(receipt).await.unwrap(), receipt_id);
    drop(store);
    cleanup(s.database);
}

async fn bound(connection: &mut Connection) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if matches!(connection.next().await.unwrap(), Some(Action::Bound { .. })) {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independent_peers_route_unicode_and_reject_spoofed_sources() {
    let s = settings();
    let store = Arc::new(SqliteStore::open(s.clone()).await.unwrap());
    let hub = Arc::new(Hub::new(store.clone(), "test-hub"));
    let (a, server_a) = tokio::io::duplex(65536);
    let (b, server_b) = tokio::io::duplex(65536);
    let h = hub.clone();
    let a_task = tokio::spawn(async move { h.serve_inbound(server_a).await });
    let h = hub.clone();
    let b_task = tokio::spawn(async move { h.serve_inbound(server_b).await });
    let mut a = Connection::new(a, Config::esme("a", "secret"), ConnectionOptions::default());
    let mut b = Connection::new(b, Config::esme("b", "secret"), ConnectionOptions::default());
    bound(&mut a).await;
    bound(&mut b).await;
    let body = MessageBody {
        source: Address::unknown("1000"),
        destination: Address::unknown("1001"),
        text: b"hello".to_vec(),
        ..MessageBody::default()
    };
    let mut body = body;
    body.data_coding = smpp_core::text::UCS2;
    body.text = "Hello 世界"
        .encode_utf16()
        .flat_map(u16::to_be_bytes)
        .collect();
    a.send_message(body.clone()).await.unwrap();
    tokio::time::timeout(TIMEOUT, async {
        match a.next().await.unwrap().unwrap() {
            Action::MessageAck { status, .. } => assert_eq!(status, CommandStatus::Ok),
            other => panic!("{other:?}"),
        }
        match b.next().await.unwrap().unwrap() {
            Action::Message {
                sequence,
                body: received,
            } => {
                assert_eq!(smpp_core::text::decode(&received).unwrap(), "Hello 世界");
                b.send(b.respond_to_message(sequence, CommandStatus::Ok, "local-1".into()))
                    .await
                    .unwrap();
            }
            other => panic!("{other:?}"),
        }
    })
    .await
    .unwrap();
    body.source = Address::unknown("1001");
    a.send_message(body).await.unwrap();
    match tokio::time::timeout(TIMEOUT, a.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Action::MessageAck { status, .. } => {
            assert_eq!(status, CommandStatus::InvalidSourceAddress)
        }
        other => panic!("{other:?}"),
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    tokio::time::timeout(TIMEOUT, async {
        a_task.await.unwrap().unwrap();
        b_task.await.unwrap().unwrap();
    })
    .await
    .unwrap();
    drop(hub);
    drop(store);
    cleanup(s.database);
}

#[tokio::test]
async fn duplicate_sessions_cannot_drain_one_peers_queue() {
    let s = settings();
    let store = Arc::new(SqliteStore::open(s.clone()).await.unwrap());
    let hub = Arc::new(Hub::new(store.clone(), "test-hub"));
    let (client, server) = tokio::io::duplex(65536);
    let h = hub.clone();
    let serving = tokio::spawn(async move { h.serve_inbound(server).await });
    let mut client = Connection::new(
        client,
        Config::esme("a", "secret"),
        ConnectionOptions::default(),
    );
    bound(&mut client).await;

    let (duplicate, server) = tokio::io::duplex(65536);
    let h = hub.clone();
    let rejected = tokio::spawn(async move { h.serve_inbound(server).await });
    let mut duplicate = Connection::new(
        duplicate,
        Config::esme("a", "secret"),
        ConnectionOptions::default(),
    );
    let event = tokio::time::timeout(TIMEOUT, duplicate.next())
        .await
        .unwrap();
    assert!(!matches!(event, Ok(Some(Action::Bound { .. }))));
    assert!(matches!(
        tokio::time::timeout(TIMEOUT, rejected)
            .await
            .unwrap()
            .unwrap(),
        Err(smpp_hub::ServiceError::AlreadyBound(_))
    ));
    assert!(hub.bound.is_bound(s.peers[0].site.id).await);

    let (outbound, _peer) = tokio::io::duplex(65536);
    assert!(matches!(
        hub.serve_outbound(outbound, s.peers[0].site.clone()).await,
        Err(smpp_hub::ServiceError::AlreadyBound(_))
    ));

    client
        .send_message(MessageBody {
            source: Address::unknown("1000"),
            destination: Address::unknown("1001"),
            text: b"still connected".to_vec(),
            ..MessageBody::default()
        })
        .await
        .unwrap();
    match tokio::time::timeout(TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Action::MessageAck { status, .. } => assert_eq!(status, CommandStatus::Ok),
        other => panic!("{other:?}"),
    }
    assert_eq!(store.queued_for_site(s.peers[1].site.id).await.unwrap(), 1);

    serving.abort();
    assert!(serving.await.unwrap_err().is_cancelled());
    assert!(!hub.bound.is_bound(s.peers[0].site.id).await);
    let (replacement, server) = tokio::io::duplex(65536);
    let h = hub.clone();
    let serving = tokio::spawn(async move { h.serve_inbound(server).await });
    let mut replacement = Connection::new(
        replacement,
        Config::esme("a", "secret"),
        ConnectionOptions::default(),
    );
    bound(&mut replacement).await;
    replacement.shutdown().await.unwrap();
    tokio::time::timeout(TIMEOUT, serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(duplicate);
    drop(client);
    drop(hub);
    drop(store);
    cleanup(s.database);
}

#[test]
fn shipped_example_has_valid_bind_credentials() {
    let s: Settings = serde_json::from_str(include_str!("../../../examples/hub.json")).unwrap();
    for peer in s.peers {
        let now = std::time::Instant::now();
        let mut session =
            smpp_core::Session::new(Config::esme(peer.site.smpp_system_id, peer.password), now);
        smpp_core::encode(&session.bind(now).unwrap()).unwrap();
    }
}

#[tokio::test]
async fn invalid_peer_settings_are_rejected_before_creating_storage() {
    let mut invalid_password = settings();
    invalid_password.peers[0].password = "too-long-password".into();
    let mut self_address = settings();
    self_address.peers[1].address = Some(self_address.listen);
    for s in [invalid_password, self_address] {
        assert!(SqliteStore::open(s.clone()).await.is_err());
        assert!(!s.database.exists());
    }
}
