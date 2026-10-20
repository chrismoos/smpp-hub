use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::pdu::command_id;
use crate::pdu::{
    Address, Bind, BindKind, BindResp, Body, CommandStatus, INTERFACE_VERSION_34, MessageBody, Pdu,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Esme,
    Smsc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Open,
    BindPending,
    Bound,
    Unbinding,
    Closed,
}

impl State {
    fn name(self) -> &'static str {
        match self {
            State::Open => "open",
            State::BindPending => "bind-pending",
            State::Bound => "bound",
            State::Unbinding => "unbinding",
            State::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub role: Role,
    pub system_id: String,
    pub password: String,
    pub bind_kind: BindKind,
    pub window: usize,
    pub enquire_interval: Duration,
    pub request_timeout: Duration,
}

impl Config {
    pub fn esme(system_id: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            role: Role::Esme,
            system_id: system_id.into(),
            password: password.into(),
            bind_kind: BindKind::Transceiver,
            window: 16,
            enquire_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }

    pub fn smsc(system_id: impl Into<String>) -> Self {
        Self {
            role: Role::Smsc,
            system_id: system_id.into(),
            password: String::new(),
            bind_kind: BindKind::Transceiver,
            window: 16,
            enquire_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send(Pdu),
    Bound {
        system_id: String,
    },
    Message {
        sequence: u32,
        body: Box<MessageBody>,
    },
    MessageAck {
        sequence: u32,
        status: CommandStatus,
        message_id: String,
    },
    BindRequest {
        sequence: u32,
        system_id: String,
        password: String,
    },
    RequestTimeout {
        sequence: u32,
    },
    Closed,
}

#[derive(Debug, Clone)]
struct Pending {
    kind: PendingKind,
    sent_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Bind,
    Message,
    EnquireLink,
    Unbind,
}

pub struct Session {
    config: Config,
    state: State,
    next_sequence: u32,
    outstanding: HashMap<u32, Pending>,
    peer_system_id: String,
    last_activity: Instant,
}

impl Session {
    pub fn new(config: Config, now: Instant) -> Self {
        Self {
            config,
            state: State::Open,
            next_sequence: 1,
            outstanding: HashMap::new(),
            peer_system_id: String::new(),
            last_activity: now,
        }
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn role(&self) -> Role {
        self.config.role
    }

    pub fn peer_system_id(&self) -> &str {
        &self.peer_system_id
    }

    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }

    // SMPP v3.4 §5.1.4 reserves zero and bounds sequence numbers at 0x7FFFFFFF.
    fn take_sequence(&mut self) -> u32 {
        const MAX_SEQUENCE: u32 = 0x7FFF_FFFF;
        loop {
            let sequence = self.next_sequence;
            self.next_sequence = if sequence == MAX_SEQUENCE {
                1
            } else {
                sequence + 1
            };
            if !self.outstanding.contains_key(&sequence) {
                return sequence;
            }
        }
    }

    fn track(&mut self, sequence: u32, kind: PendingKind, now: Instant) {
        self.outstanding
            .insert(sequence, Pending { kind, sent_at: now });
    }

    pub fn bind(&mut self, now: Instant) -> Result<Pdu> {
        if self.config.role != Role::Esme {
            return Err(Error::InvalidSessionState {
                operation: "bind",
                state: "smsc role",
            });
        }
        if self.state != State::Open {
            return Err(Error::InvalidSessionState {
                operation: "bind",
                state: self.state.name(),
            });
        }
        let sequence = self.take_sequence();
        let pdu = Pdu::request(
            sequence,
            Body::Bind(Bind {
                kind: self.config.bind_kind,
                system_id: self.config.system_id.clone(),
                password: self.config.password.clone(),
                system_type: String::new(),
                interface_version: INTERFACE_VERSION_34,
                address_range: Address::default(),
            }),
        );
        self.track(sequence, PendingKind::Bind, now);
        self.state = State::BindPending;
        self.last_activity = now;
        Ok(pdu)
    }

    /// Accepts a bind the caller has authenticated.
    pub fn accept_bind(&mut self, sequence: u32, peer_system_id: String, now: Instant) -> Pdu {
        self.peer_system_id = peer_system_id;
        self.state = State::Bound;
        self.last_activity = now;
        Pdu::response(
            sequence,
            CommandStatus::Ok,
            Body::BindResp(BindResp {
                kind: self.config.bind_kind,
                system_id: self.config.system_id.clone(),
                sc_interface_version: Some(INTERFACE_VERSION_34),
            }),
        )
    }

    pub fn reject_bind(&mut self, sequence: u32, status: CommandStatus) -> Pdu {
        self.state = State::Closed;
        Pdu::response(
            sequence,
            status,
            Body::BindResp(BindResp {
                kind: self.config.bind_kind,
                system_id: String::new(),
                sc_interface_version: None,
            }),
        )
    }

    pub fn send_message(&mut self, mut body: MessageBody, now: Instant) -> Result<(u32, Pdu)> {
        if self.state != State::Bound {
            return Err(Error::InvalidSessionState {
                operation: "send a message",
                state: self.state.name(),
            });
        }
        if self.outstanding.len() >= self.config.window {
            return Err(Error::WindowFull);
        }
        let permitted = match self.config.role {
            Role::Esme => self.config.bind_kind != BindKind::Receiver,
            Role::Smsc => self.config.bind_kind != BindKind::Transmitter,
        };
        if !permitted {
            return Err(Error::InvalidSessionState {
                operation: "send a message",
                state: "bind does not permit this direction",
            });
        }
        if self.config.role == Role::Esme && body.is_delivery_receipt() {
            return Err(Error::InvalidValue {
                context: "submit_sm",
                reason: "SMSC delivery receipts require deliver_sm",
            });
        }
        if self.config.role == Role::Smsc {
            // SMPP v3.4 §4.6.1 reserves these deliver_sm fields.
            body.schedule_delivery_time.clear();
            body.validity_period.clear();
            body.replace_if_present = 0;
            body.sm_default_msg_id = 0;
            body.registered_delivery = 0;
        }
        let sequence = self.take_sequence();
        let body = Box::new(body);
        let pdu = Pdu::request(
            sequence,
            match self.config.role {
                Role::Esme => Body::SubmitSm(body),
                Role::Smsc => Body::DeliverSm(body),
            },
        );
        self.track(sequence, PendingKind::Message, now);
        self.last_activity = now;
        Ok((sequence, pdu))
    }

    pub fn unbind(&mut self, now: Instant) -> Result<Pdu> {
        if self.state != State::Bound {
            return Err(Error::InvalidSessionState {
                operation: "unbind",
                state: self.state.name(),
            });
        }
        let sequence = self.take_sequence();
        self.track(sequence, PendingKind::Unbind, now);
        self.state = State::Unbinding;
        self.last_activity = now;
        Ok(Pdu::request(sequence, Body::Unbind))
    }

    pub fn handle(&mut self, pdu: Pdu, now: Instant) -> Vec<Action> {
        self.last_activity = now;
        let mut actions = Vec::new();
        if matches!(
            (&pdu.body, self.config.role),
            (Body::SubmitSm(_), Role::Esme) | (Body::DeliverSm(_), Role::Smsc)
        ) {
            return vec![Action::Send(Pdu::response(
                pdu.sequence_number,
                CommandStatus::InvalidCommandId,
                Body::GenericNack,
            ))];
        }

        if pdu.body.is_response() {
            let Some(pending) = self.outstanding.get(&pdu.sequence_number) else {
                return actions;
            };
            let expected = match pending.kind {
                PendingKind::Bind => self.config.bind_kind.response_command_id(),
                PendingKind::Message => {
                    if self.config.role == Role::Esme {
                        command_id::SUBMIT_SM_RESP
                    } else {
                        command_id::DELIVER_SM_RESP
                    }
                }
                PendingKind::EnquireLink => command_id::ENQUIRE_LINK_RESP,
                PendingKind::Unbind => command_id::UNBIND_RESP,
            };
            if !matches!(pdu.body, Body::GenericNack) && pdu.command_id() != expected {
                return actions;
            }
            let pending = self
                .outstanding
                .remove(&pdu.sequence_number)
                .expect("pending request was checked");
            if matches!(pdu.body, Body::GenericNack) {
                if pending.kind == PendingKind::Message {
                    actions.push(Action::MessageAck {
                        sequence: pdu.sequence_number,
                        status: if pdu.status == CommandStatus::Ok {
                            CommandStatus::SystemError
                        } else {
                            pdu.status
                        },
                        message_id: String::new(),
                    });
                } else {
                    self.state = State::Closed;
                    actions.push(Action::Closed);
                }
                return actions;
            }
            match (pending.kind, &pdu.body) {
                (PendingKind::Bind, Body::BindResp(resp)) => {
                    if pdu.status == CommandStatus::Ok {
                        self.peer_system_id = resp.system_id.clone();
                        self.state = State::Bound;
                        actions.push(Action::Bound {
                            system_id: resp.system_id.clone(),
                        });
                    } else {
                        self.state = State::Closed;
                        actions.push(Action::Closed);
                    }
                }
                (PendingKind::Message, Body::SubmitSmResp { message_id })
                | (PendingKind::Message, Body::DeliverSmResp { message_id }) => {
                    actions.push(Action::MessageAck {
                        sequence: pdu.sequence_number,
                        status: pdu.status,
                        message_id: message_id.clone(),
                    });
                }
                (PendingKind::Unbind, Body::UnbindResp) => {
                    self.state = State::Closed;
                    actions.push(Action::Closed);
                }
                (PendingKind::EnquireLink, _) => {}
                _ => {}
            }
            return actions;
        }

        match pdu.body {
            Body::Bind(bind) => {
                if self.config.role != Role::Smsc || self.state != State::Open {
                    actions.push(Action::Send(Pdu::response(
                        pdu.sequence_number,
                        CommandStatus::IncorrectBindStatus,
                        Body::GenericNack,
                    )));
                    return actions;
                }
                if bind.interface_version != INTERFACE_VERSION_34 {
                    actions.push(Action::Send(Pdu::response(
                        pdu.sequence_number,
                        CommandStatus::BindFailed,
                        Body::BindResp(BindResp {
                            kind: bind.kind,
                            system_id: String::new(),
                            sc_interface_version: None,
                        }),
                    )));
                    self.state = State::Closed;
                    actions.push(Action::Closed);
                    return actions;
                }
                self.config.bind_kind = bind.kind;
                actions.push(Action::BindRequest {
                    sequence: pdu.sequence_number,
                    system_id: bind.system_id,
                    password: bind.password,
                });
            }
            Body::SubmitSm(body) | Body::DeliverSm(body) => {
                let permitted = match self.config.role {
                    Role::Esme => self.config.bind_kind != BindKind::Transmitter,
                    Role::Smsc => self.config.bind_kind != BindKind::Receiver,
                };
                if self.state != State::Bound || !permitted {
                    actions.push(Action::Send(Pdu::response(
                        pdu.sequence_number,
                        CommandStatus::IncorrectBindStatus,
                        match self.config.role {
                            Role::Esme => Body::DeliverSmResp {
                                message_id: String::new(),
                            },
                            Role::Smsc => Body::SubmitSmResp {
                                message_id: String::new(),
                            },
                        },
                    )));
                    return actions;
                }
                actions.push(Action::Message {
                    sequence: pdu.sequence_number,
                    body,
                });
            }
            Body::EnquireLink => {
                actions.push(Action::Send(Pdu::response(
                    pdu.sequence_number,
                    CommandStatus::Ok,
                    Body::EnquireLinkResp,
                )));
            }
            Body::Unbind => {
                self.state = State::Closed;
                actions.push(Action::Send(Pdu::response(
                    pdu.sequence_number,
                    CommandStatus::Ok,
                    Body::UnbindResp,
                )));
                actions.push(Action::Closed);
            }
            Body::GenericNack => {}
            other => {
                actions.push(Action::Send(Pdu::response(
                    pdu.sequence_number,
                    CommandStatus::InvalidCommandId,
                    Body::GenericNack,
                )));
                let _ = other;
            }
        }
        actions
    }

    pub fn respond_to_message(
        &self,
        sequence: u32,
        status: CommandStatus,
        message_id: String,
    ) -> Pdu {
        message_response(self.config.role, sequence, status, message_id)
    }

    pub fn tick(&mut self, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();

        let expired: Vec<u32> = self
            .outstanding
            .iter()
            .filter(|(_, pending)| {
                now.duration_since(pending.sent_at) >= self.config.request_timeout
            })
            .map(|(seq, _)| *seq)
            .collect();
        for sequence in expired {
            let pending = self.outstanding.remove(&sequence);
            match pending.map(|p| p.kind) {
                Some(PendingKind::Bind) | Some(PendingKind::Unbind) => {
                    self.state = State::Closed;
                    actions.push(Action::Closed);
                }
                Some(PendingKind::EnquireLink) => {
                    self.state = State::Closed;
                    actions.push(Action::Closed);
                }
                _ => actions.push(Action::RequestTimeout { sequence }),
            }
        }

        if self.state == State::Bound
            && now.duration_since(self.last_activity) >= self.config.enquire_interval
            && !self
                .outstanding
                .values()
                .any(|p| p.kind == PendingKind::EnquireLink)
        {
            let sequence = self.take_sequence();
            self.track(sequence, PendingKind::EnquireLink, now);
            self.last_activity = now;
            actions.push(Action::Send(Pdu::request(sequence, Body::EnquireLink)));
        }

        actions
    }
}

pub(crate) fn message_response(
    role: Role,
    sequence: u32,
    status: CommandStatus,
    message_id: String,
) -> Pdu {
    Pdu::response(
        sequence,
        status,
        match role {
            // SMPP v3.4 §4.6.2 leaves deliver_sm_resp.message_id unused.
            Role::Esme => Body::DeliverSmResp {
                message_id: String::new(),
            },
            Role::Smsc => Body::SubmitSmResp { message_id },
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::Address;

    fn body(text: &str) -> MessageBody {
        MessageBody {
            source: Address::unknown("7025550100"),
            destination: Address::unknown("4155550118"),
            text: text.as_bytes().to_vec(),
            ..MessageBody::default()
        }
    }

    fn bind_pair(now: Instant) -> (Session, Session) {
        let mut esme = Session::new(Config::esme("site-a", "pw"), now);
        let mut smsc = Session::new(Config::smsc("hub"), now);

        let bind = esme.bind(now).expect("bind");
        let actions = smsc.handle(bind, now);
        let Action::BindRequest {
            sequence,
            system_id,
            password,
        } = &actions[0]
        else {
            panic!("expected a bind request, got {actions:?}");
        };
        assert_eq!(system_id, "site-a");
        assert_eq!(password, "pw");

        let resp = smsc.accept_bind(*sequence, system_id.clone(), now);
        let actions = esme.handle(resp, now);
        assert!(matches!(actions[0], Action::Bound { .. }));
        assert_eq!(esme.state(), State::Bound);
        assert_eq!(smsc.state(), State::Bound);
        (esme, smsc)
    }

    #[test]
    fn bind_completes_and_records_the_peer() {
        let now = Instant::now();
        let (esme, smsc) = bind_pair(now);
        assert_eq!(esme.peer_system_id(), "hub");
        assert_eq!(smsc.peer_system_id(), "site-a");
    }

    #[test]
    fn delivery_fields_and_response_ids_follow_the_smsc_direction() {
        let now = Instant::now();
        let (mut esme, mut smsc) = bind_pair(now);
        let mut message = body("hello");
        message.registered_delivery = 1;
        message.validity_period = "value".into();
        message.schedule_delivery_time = "value".into();
        message.replace_if_present = 1;
        message.sm_default_msg_id = 1;
        let (sequence, pdu) = smsc.send_message(message, now).unwrap();
        let Body::DeliverSm(delivery) = &pdu.body else {
            panic!("deliver_sm")
        };
        assert_eq!(delivery.registered_delivery, 0);
        assert!(delivery.validity_period.is_empty());
        assert!(delivery.schedule_delivery_time.is_empty());
        assert_eq!(delivery.replace_if_present, 0);
        assert_eq!(delivery.sm_default_msg_id, 0);
        assert!(
            matches!(esme.respond_to_message(sequence,CommandStatus::Ok,"ignored".into()).body,Body::DeliverSmResp{message_id} if message_id.is_empty())
        );
        let mut receipt = body("");
        receipt.esm_class = crate::esm_class::DELIVERY_RECEIPT;
        assert!(esme.send_message(receipt, now).is_err());
        assert!(matches!(
            &smsc.handle(pdu, now)[0],
            Action::Send(Pdu {
                status: CommandStatus::InvalidCommandId,
                ..
            })
        ));
    }

    #[test]
    fn a_rejected_bind_closes_the_session() {
        let now = Instant::now();
        let mut esme = Session::new(Config::esme("site-a", "wrong"), now);
        let mut smsc = Session::new(Config::smsc("hub"), now);
        let bind = esme.bind(now).unwrap();
        let actions = smsc.handle(bind, now);
        let Action::BindRequest { sequence, .. } = actions[0] else {
            panic!("expected bind request");
        };
        let resp = smsc.reject_bind(sequence, CommandStatus::InvalidPassword);
        let actions = esme.handle(resp, now);
        assert_eq!(actions, vec![Action::Closed]);
        assert_eq!(esme.state(), State::Closed);
    }

    #[test]
    fn esme_submits_and_smsc_delivers() {
        let now = Instant::now();
        let (mut esme, mut smsc) = bind_pair(now);

        let (seq, pdu) = esme.send_message(body("up"), now).unwrap();
        assert!(matches!(pdu.body, Body::SubmitSm(_)));
        let actions = smsc.handle(pdu, now);
        let Action::Message {
            sequence,
            body: received,
        } = &actions[0]
        else {
            panic!("expected a message, got {actions:?}");
        };
        assert_eq!(received.text, b"up");
        let ack = smsc.respond_to_message(*sequence, CommandStatus::Ok, "m-1".to_string());
        let actions = esme.handle(ack, now);
        assert_eq!(
            actions,
            vec![Action::MessageAck {
                sequence: seq,
                status: CommandStatus::Ok,
                message_id: "m-1".to_string(),
            }]
        );

        let (_, pdu) = smsc.send_message(body("down"), now).unwrap();
        assert!(matches!(pdu.body, Body::DeliverSm(_)));
        let actions = esme.handle(pdu, now);
        assert!(matches!(actions[0], Action::Message { .. }));
    }

    #[test]
    fn the_window_bounds_outstanding_requests() {
        let now = Instant::now();
        let mut config = Config::esme("site-a", "pw");
        config.window = 2;
        let mut esme = Session::new(config, now);
        let mut smsc = Session::new(Config::smsc("hub"), now);
        let bind = esme.bind(now).unwrap();
        let actions = smsc.handle(bind, now);
        let Action::BindRequest { sequence, .. } = actions[0] else {
            panic!()
        };
        esme.handle(smsc.accept_bind(sequence, "site-a".into(), now), now);

        esme.send_message(body("1"), now).unwrap();
        esme.send_message(body("2"), now).unwrap();
        assert_eq!(esme.send_message(body("3"), now), Err(Error::WindowFull));
    }

    #[test]
    fn an_idle_link_sends_enquire_link_and_the_peer_answers() {
        let now = Instant::now();
        let (mut esme, mut smsc) = bind_pair(now);
        let later = now + Duration::from_secs(31);

        let actions = esme.tick(later);
        let Action::Send(pdu) = &actions[0] else {
            panic!("expected an enquire_link, got {actions:?}");
        };
        assert!(matches!(pdu.body, Body::EnquireLink));

        let actions = smsc.handle(pdu.clone(), later);
        let Action::Send(resp) = &actions[0] else {
            panic!("expected a response");
        };
        assert!(matches!(resp.body, Body::EnquireLinkResp));
        assert!(esme.handle(resp.clone(), later).is_empty());
        assert_eq!(esme.outstanding(), 0);
    }

    #[test]
    fn an_unanswered_message_times_out_without_closing_the_link() {
        let now = Instant::now();
        let (mut esme, _smsc) = bind_pair(now);
        let (seq, _) = esme.send_message(body("x"), now).unwrap();
        let actions = esme.tick(now + Duration::from_secs(31));
        assert!(actions.contains(&Action::RequestTimeout { sequence: seq }));
        assert_eq!(esme.state(), State::Bound);
    }

    #[test]
    fn an_unanswered_enquire_link_closes_the_link() {
        let now = Instant::now();
        let (mut esme, _smsc) = bind_pair(now);
        let idle = now + Duration::from_secs(31);
        esme.tick(idle);
        let actions = esme.tick(idle + Duration::from_secs(31));
        assert!(actions.contains(&Action::Closed));
        assert_eq!(esme.state(), State::Closed);
    }

    #[test]
    fn unbind_closes_both_sides() {
        let now = Instant::now();
        let (mut esme, mut smsc) = bind_pair(now);
        let pdu = esme.unbind(now).unwrap();
        let actions = smsc.handle(pdu, now);
        let Action::Send(resp) = &actions[0] else {
            panic!("expected unbind_resp");
        };
        assert_eq!(smsc.state(), State::Closed);
        let actions = esme.handle(resp.clone(), now);
        assert_eq!(actions, vec![Action::Closed]);
    }

    #[test]
    fn messages_are_refused_before_bind() {
        let now = Instant::now();
        let mut esme = Session::new(Config::esme("a", "b"), now);
        assert!(esme.send_message(body("x"), now).is_err());
    }

    #[test]
    fn a_message_before_bind_is_answered_with_an_error() {
        let now = Instant::now();
        let mut smsc = Session::new(Config::smsc("hub"), now);
        let pdu = Pdu::request(1, Body::SubmitSm(Box::new(body("early"))));
        let actions = smsc.handle(pdu, now);
        let Action::Send(resp) = &actions[0] else {
            panic!("expected an error response");
        };
        assert_eq!(resp.status, CommandStatus::IncorrectBindStatus);
    }
}

#[cfg(test)]
#[path = "session/conformance.rs"]
mod conformance;
