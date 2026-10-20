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
fn shipped_examples_have_valid_bind_credentials() {
    for example in [
        include_str!("../../../examples/hub.json"),
        include_str!("../../../examples/upstream.json"),
    ] {
        let s: Settings = serde_json::from_str(example).unwrap();
        for peer in s.peers {
            let now = std::time::Instant::now();
            let mut session =
                smpp_core::Session::new(Config::esme(peer.site.smpp_system_id, peer.password), now);
            smpp_core::encode(&session.bind(now).unwrap()).unwrap();
        }
    }
}

#[tokio::test]
async fn the_upstream_example_keeps_local_numbers_local() {
    let mut s: Settings =
        serde_json::from_str(include_str!("../../../examples/upstream.json")).unwrap();
    s.database = settings().database;
    let store = SqliteStore::open(s.clone()).await.unwrap();
    for (number, peer) in [("1001", 0), ("1004", 1), ("1006", 2), ("7025550100", 2)] {
        assert_eq!(
            store.site_for_number(number).await.unwrap(),
            Some(s.peers[peer].site.id),
            "{number}"
        );
    }
    drop(store);
    cleanup(s.database);
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

fn peer(id: u128, name: &str, role: BindRole, numbers: &[&str]) -> Peer {
    Peer {
        site: Site {
            id: SiteId(uuid::Uuid::from_u128(id)),
            name: name.into(),
            smpp_system_id: name.into(),
            bind_role: role,
            messages_per_hour: None,
        },
        password: "secret".into(),
        numbers: numbers.iter().map(|n| n.to_string()).collect(),
        address: (role == BindRole::HubBindsToSite).then(|| "127.0.0.1:9".parse().unwrap()),
    }
}

fn hub_settings(peers: Vec<Peer>) -> Settings {
    Settings {
        peers,
        ..settings()
    }
}

#[tokio::test]
async fn exact_numbers_win_and_patterns_match_whole_numbers_in_order() {
    let s = hub_settings(vec![
        peer(1, "wide", BindRole::SiteBindsToHub, &["70[0-9]+"]),
        peer(2, "exact", BindRole::SiteBindsToHub, &["7025550100"]),
        peer(3, "rest", BindRole::SiteBindsToHub, &[".*"]),
    ]);
    let store = SqliteStore::open(s.clone()).await.unwrap();
    let owner = |n: &'static str| {
        let store = &store;
        async move {
            store
                .site_for_number(n)
                .await
                .unwrap()
                .unwrap()
                .as_uuid()
                .as_u128()
        }
    };
    assert_eq!(
        owner("7025550100").await,
        2,
        "an exact number beats an earlier pattern"
    );
    assert_eq!(owner("7025550101").await, 1);
    assert_eq!(
        owner("17025550101").await,
        3,
        "patterns match the whole number"
    );
    drop(store);
    cleanup(s.database);
}

#[tokio::test]
async fn bad_number_patterns_are_rejected_at_startup() {
    for numbers in [
        &["70[0-9"][..],
        &[""][..],
        &["1000", "1000"][..],
        &["911"][..],
    ] {
        let s = hub_settings(vec![peer(1, "a", BindRole::SiteBindsToHub, numbers)]);
        assert!(SqliteStore::open(s.clone()).await.is_err(), "{numbers:?}");
        assert!(!s.database.exists());
    }
}

async fn ack(connection: &mut Connection) -> CommandStatus {
    match tokio::time::timeout(TIMEOUT, connection.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Action::MessageAck { status, .. } => status,
        other => panic!("{other:?}"),
    }
}

async fn receive(connection: &mut Connection) -> MessageBody {
    match tokio::time::timeout(TIMEOUT, connection.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Action::Message { sequence, body } => {
            connection
                .send(connection.respond_to_message(sequence, CommandStatus::Ok, "id".into()))
                .await
                .unwrap();
            *body
        }
        other => panic!("{other:?}"),
    }
}

fn text(source: &str, destination: &str) -> MessageBody {
    MessageBody {
        source: Address::unknown(source),
        destination: Address::unknown(destination),
        text: format!("{source} to {destination}").into_bytes(),
        ..MessageBody::default()
    }
}

async fn client(hub: &Arc<Hub>, system_id: &str) -> Connection {
    let (near, far) = tokio::io::duplex(65536);
    let h = hub.clone();
    tokio::spawn(async move { h.serve_inbound(far).await });
    let mut connection = Connection::new(
        near,
        Config::esme(system_id, "secret"),
        ConnectionOptions::default(),
    );
    bound(&mut connection).await;
    connection
}

/// A local hub serving two networks forwards everything else to an upstream
/// hub through a catch-all, the way several networks share one registry site.
#[tokio::test]
async fn a_catch_all_peer_links_local_networks_to_an_upstream_hub() {
    let upstream_settings = hub_settings(vec![
        peer(
            10,
            "home",
            BindRole::SiteBindsToHub,
            &["1000", "1001", "3000"],
        ),
        peer(11, "remote", BindRole::SiteBindsToHub, &["2000"]),
    ]);
    let local_settings = hub_settings(vec![
        peer(1, "net-a", BindRole::SiteBindsToHub, &["1000"]),
        peer(2, "net-b", BindRole::SiteBindsToHub, &["1001"]),
        Peer {
            site: Site {
                smpp_system_id: "home".into(),
                ..peer(3, "registry", BindRole::HubBindsToSite, &[]).site
            },
            ..peer(3, "registry", BindRole::HubBindsToSite, &[".*"])
        },
    ]);
    let upstream_store = Arc::new(SqliteStore::open(upstream_settings.clone()).await.unwrap());
    let local_store = Arc::new(SqliteStore::open(local_settings.clone()).await.unwrap());
    let upstream = Arc::new(Hub::new(upstream_store.clone(), "registry"));
    let local = Arc::new(Hub::new(local_store.clone(), "local"));

    let (near, far) = tokio::io::duplex(65536);
    let h = upstream.clone();
    tokio::spawn(async move { h.serve_inbound(far).await });
    let h = local.clone();
    let registry_peer = local_settings.peers[2].site.clone();
    tokio::spawn(async move { h.serve_outbound(near, registry_peer).await });

    let mut a = client(&local, "net-a").await;
    let mut b = client(&local, "net-b").await;
    let mut remote = client(&upstream, "remote").await;

    a.send_message(text("1000", "1001")).await.unwrap();
    assert_eq!(ack(&mut a).await, CommandStatus::Ok);
    assert_eq!(receive(&mut b).await.source.digits, "1000");
    assert_eq!(
        upstream_store
            .queued_for_site(SiteId(uuid::Uuid::from_u128(11)))
            .await
            .unwrap(),
        0,
        "local traffic stays local"
    );

    a.send_message(text("1000", "2000")).await.unwrap();
    assert_eq!(ack(&mut a).await, CommandStatus::Ok);
    assert_eq!(receive(&mut remote).await.source.digits, "1000");

    remote.send_message(text("2000", "1001")).await.unwrap();
    assert_eq!(ack(&mut remote).await, CommandStatus::Ok);
    assert_eq!(receive(&mut b).await.source.digits, "2000");

    a.send_message(text("2000", "1001")).await.unwrap();
    assert_eq!(
        ack(&mut a).await,
        CommandStatus::InvalidSourceAddress,
        "a local network cannot send as another operator's number"
    );

    remote.send_message(text("2000", "3000")).await.unwrap();
    let id = match tokio::time::timeout(TIMEOUT, remote.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Action::MessageAck {
            status: CommandStatus::Ok,
            message_id,
            ..
        } => MessageId(message_id.parse().unwrap()),
        other => panic!("{other:?}"),
    };
    tokio::time::timeout(TIMEOUT, async {
        while upstream_store.message(id).await.unwrap().unwrap().state != MessageState::Failed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the local hub refuses a number only its catch-all back to the sender matches");
    assert_eq!(
        local_store
            .queued_for_site(SiteId(uuid::Uuid::from_u128(3)))
            .await
            .unwrap(),
        0
    );

    for path in [upstream_settings.database, local_settings.database] {
        cleanup(path);
    }
}
