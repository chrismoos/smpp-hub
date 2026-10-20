use super::*;
fn pair(kind: BindKind) -> (Session, Session) {
    let now = Instant::now();
    let mut config = Config::esme("site", "password");
    config.bind_kind = kind;
    let mut esme = Session::new(config, now);
    let mut smsc = Session::new(Config::smsc("hub"), now);
    let bind = esme.bind(now).unwrap();
    let action = smsc.handle(bind, now).remove(0);
    let Action::BindRequest {
        sequence,
        system_id,
        ..
    } = action
    else {
        panic!()
    };
    let response = smsc.accept_bind(sequence, system_id, now);
    esme.handle(response, now);
    (esme, smsc)
}
#[test]
fn wrong_response_opcode_does_not_consume_pending_submission() {
    let (mut esme, _) = pair(BindKind::Transceiver);
    let now = Instant::now();
    let (sequence, _) = esme.send_message(MessageBody::default(), now).unwrap();
    assert!(
        esme.handle(
            Pdu::response(
                sequence,
                CommandStatus::Ok,
                Body::DeliverSmResp {
                    message_id: String::new()
                }
            ),
            now
        )
        .is_empty()
    );
    assert_eq!(esme.outstanding(), 1);
    assert!(
        matches!(esme.handle(Pdu::response(sequence,CommandStatus::Ok,Body::SubmitSmResp{message_id:"id".into()}),now).as_slice(),[Action::MessageAck{message_id,..}]if message_id=="id")
    );
}
#[test]
fn generic_nack_resolves_the_original_submission() {
    let (mut esme, _) = pair(BindKind::Transceiver);
    let now = Instant::now();
    let (sequence, _) = esme.send_message(MessageBody::default(), now).unwrap();
    assert!(matches!(
        esme.handle(
            Pdu::response(sequence, CommandStatus::InvalidCommandId, Body::GenericNack),
            now
        )
        .as_slice(),
        [Action::MessageAck {
            status: CommandStatus::InvalidCommandId,
            ..
        }]
    ));
    assert_eq!(esme.outstanding(), 0);
}
#[test]
fn receiver_and_transmitter_binds_enforce_message_permissions() {
    let (mut receiver, mut smsc) = pair(BindKind::Receiver);
    let now = Instant::now();
    assert!(receiver.send_message(MessageBody::default(), now).is_err());
    assert!(matches!(
        smsc.handle(Pdu::request(2, Body::SubmitSm(Box::default())), now)
            .as_slice(),
        [Action::Send(Pdu {
            status: CommandStatus::IncorrectBindStatus,
            ..
        })]
    ));
    let (mut transmitter, mut smsc) = pair(BindKind::Transmitter);
    assert!(smsc.send_message(MessageBody::default(), now).is_err());
    assert!(matches!(
        transmitter
            .handle(Pdu::request(2, Body::DeliverSm(Box::default())), now)
            .as_slice(),
        [Action::Send(Pdu {
            status: CommandStatus::IncorrectBindStatus,
            ..
        })]
    ));
}
#[test]
fn unsupported_bind_version_is_rejected_before_authentication() {
    let now = Instant::now();
    let mut esme = Session::new(Config::esme("site", "password"), now);
    let mut smsc = Session::new(Config::smsc("hub"), now);
    let mut bind = esme.bind(now).unwrap();
    let Body::Bind(body) = &mut bind.body else {
        panic!()
    };
    body.interface_version = 0x50;
    let actions = smsc.handle(bind, now);
    assert!(matches!(
        actions.as_slice(),
        [
            Action::Send(Pdu {
                status: CommandStatus::BindFailed,
                ..
            }),
            Action::Closed
        ]
    ));
}
#[test]
fn sequence_wrap_includes_maximum_and_skips_in_flight_values() {
    let (mut esme, _) = pair(BindKind::Transceiver);
    let now = Instant::now();
    esme.next_sequence = 0x7fff_ffff;
    let (maximum, _) = esme.send_message(MessageBody::default(), now).unwrap();
    assert_eq!(maximum, 0x7fff_ffff);
    let (first, _) = esme.send_message(MessageBody::default(), now).unwrap();
    assert_eq!(first, 1);
    esme.next_sequence = 0x7fff_ffff;
    let (next, _) = esme.send_message(MessageBody::default(), now).unwrap();
    assert_eq!(next, 2);
}
#[test]
fn failed_response_is_header_only_and_unknown_status_survives() {
    let pdu = Pdu::response(
        7,
        CommandStatus::Other(0x1234),
        Body::SubmitSmResp {
            message_id: "not-returned".into(),
        },
    );
    let bytes = crate::codec::encode(&pdu).unwrap();
    assert_eq!(
        bytes,
        vec![0, 0, 0, 16, 0x80, 0, 0, 4, 0, 0, 0x12, 0x34, 0, 0, 0, 7]
    );
    let decoded = crate::codec::decode(&bytes).unwrap();
    assert_eq!(decoded.status.as_u32(), 0x1234);
    assert!(matches!(decoded.body,Body::SubmitSmResp{message_id}if message_id.is_empty()));
}
#[test]
fn unsupported_request_gets_generic_nack_with_its_sequence() {
    let bytes = [0, 0, 0, 16, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 9];
    let request = crate::codec::decode(&bytes).unwrap();
    let (mut esme, _) = pair(BindKind::Transceiver);
    assert!(matches!(
        esme.handle(request, Instant::now()).as_slice(),
        [Action::Send(Pdu {
            sequence_number: 9,
            status: CommandStatus::InvalidCommandId,
            body: Body::GenericNack
        })]
    ));
}
