//! SMPP v3.4 protocol data units.
//!
//! SMPP v3.4 §§4–5 define the supported PDUs and fields.

use crate::error::{Error, Result};

pub const HEADER_LEN: usize = 16;

pub const MAX_PDU_LEN: u32 = 256 * 1024;

pub const INTERFACE_VERSION_34: u8 = 0x34;

/// Command identifiers (SMPP v3.4 §5.1.2.1).
pub mod command_id {
    pub const GENERIC_NACK: u32 = 0x8000_0000;
    pub const BIND_RECEIVER: u32 = 0x0000_0001;
    pub const BIND_RECEIVER_RESP: u32 = 0x8000_0001;
    pub const BIND_TRANSMITTER: u32 = 0x0000_0002;
    pub const BIND_TRANSMITTER_RESP: u32 = 0x8000_0002;
    pub const SUBMIT_SM: u32 = 0x0000_0004;
    pub const SUBMIT_SM_RESP: u32 = 0x8000_0004;
    pub const DELIVER_SM: u32 = 0x0000_0005;
    pub const DELIVER_SM_RESP: u32 = 0x8000_0005;
    pub const UNBIND: u32 = 0x0000_0006;
    pub const UNBIND_RESP: u32 = 0x8000_0006;
    pub const BIND_TRANSCEIVER: u32 = 0x0000_0009;
    pub const BIND_TRANSCEIVER_RESP: u32 = 0x8000_0009;
    pub const ENQUIRE_LINK: u32 = 0x0000_0015;
    pub const ENQUIRE_LINK_RESP: u32 = 0x8000_0015;
}

/// SMPP v3.4 §5.1.3 defines command status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum CommandStatus {
    Ok = 0x0000_0000,
    InvalidMsgLength = 0x0000_0001,
    InvalidCommandId = 0x0000_0003,
    IncorrectBindStatus = 0x0000_0004,
    AlreadyBound = 0x0000_0005,
    SystemError = 0x0000_0008,
    InvalidSourceAddress = 0x0000_000A,
    InvalidDestAddress = 0x0000_000B,
    BindFailed = 0x0000_000D,
    InvalidPassword = 0x0000_000E,
    InvalidSystemId = 0x0000_000F,
    MessageQueueFull = 0x0000_0014,
    SubmitFailed = 0x0000_0045,
    Throttled = 0x0000_0058,
    TransientAppError = 0x0000_0064,
    Other(u32),
}

impl CommandStatus {
    pub fn as_u32(self) -> u32 {
        match self {
            Self::Ok => 0x0000_0000,
            Self::InvalidMsgLength => 0x0000_0001,
            Self::InvalidCommandId => 0x0000_0003,
            Self::IncorrectBindStatus => 0x0000_0004,
            Self::AlreadyBound => 0x0000_0005,
            Self::SystemError => 0x0000_0008,
            Self::InvalidSourceAddress => 0x0000_000A,
            Self::InvalidDestAddress => 0x0000_000B,
            Self::BindFailed => 0x0000_000D,
            Self::InvalidPassword => 0x0000_000E,
            Self::InvalidSystemId => 0x0000_000F,
            Self::MessageQueueFull => 0x0000_0014,
            Self::SubmitFailed => 0x0000_0045,
            Self::Throttled => 0x0000_0058,
            Self::TransientAppError => 0x0000_0064,
            Self::Other(value) => value,
        }
    }

    pub fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            0x0000_0000 => Self::Ok,
            0x0000_0001 => Self::InvalidMsgLength,
            0x0000_0003 => Self::InvalidCommandId,
            0x0000_0004 => Self::IncorrectBindStatus,
            0x0000_0005 => Self::AlreadyBound,
            0x0000_0008 => Self::SystemError,
            0x0000_000A => Self::InvalidSourceAddress,
            0x0000_000B => Self::InvalidDestAddress,
            0x0000_000D => Self::BindFailed,
            0x0000_000F => Self::InvalidSystemId,
            0x0000_000E => Self::InvalidPassword,
            0x0000_0014 => Self::MessageQueueFull,
            0x0000_0045 => Self::SubmitFailed,
            0x0000_0058 => Self::Throttled,
            0x0000_0064 => Self::TransientAppError,
            _ => return None,
        })
    }

    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::SystemError
                | Self::MessageQueueFull
                | Self::Throttled
                | Self::TransientAppError
                | Self::SubmitFailed
        )
    }
}

pub const DELIVERY_RECEIPT_MASK: u8 = 0x03;
pub const DELIVERY_RECEIPT_FAILURE_ONLY: u8 = 0x02;

/// SMPP v3.4 §5.2.28 defines message_state values.
pub mod message_state {
    pub const ENROUTE: u8 = 1;
    pub const DELIVERED: u8 = 2;
    pub const EXPIRED: u8 = 3;
    pub const DELETED: u8 = 4;
    pub const UNDELIVERABLE: u8 = 5;
    pub const ACCEPTED: u8 = 6;
    pub const UNKNOWN: u8 = 7;
    pub const REJECTED: u8 = 8;
    pub fn is_final(value: u8) -> bool {
        matches!(
            value,
            DELIVERED | EXPIRED | DELETED | UNDELIVERABLE | REJECTED
        )
    }
}

/// SMPP v3.4 §5.3 defines standard tags. IS637 tags use the vendor range 0x1400–0x3FFF.
pub mod tlv_tag {
    pub const MESSAGE_PAYLOAD: u16 = 0x0424;
    pub const USER_MESSAGE_REFERENCE: u16 = 0x0204;
    pub const SAR_MSG_REF_NUM: u16 = 0x020C;
    pub const SAR_TOTAL_SEGMENTS: u16 = 0x020E;
    pub const SAR_SEGMENT_SEQNUM: u16 = 0x020F;
    pub const RECEIPTED_MESSAGE_ID: u16 = 0x001E;
    pub const MESSAGE_STATE: u16 = 0x0427;
    pub const SC_INTERFACE_VERSION: u16 = 0x0210;
    /// Native C.S0015 bearer data carried in a vendor TLV.
    pub const IS637_BEARER_DATA: u16 = 0x1400;
    /// C.S0015 teleservice identifier for the bearer data above.
    pub const IS637_TELESERVICE: u16 = 0x1401;
}

/// `esm_class` bits this implementation reads or sets (SMPP v3.4 §5.2.12).
pub mod esm_class {
    pub const DELIVERY_RECEIPT: u8 = 0x04;
    pub const UDHI: u8 = 0x40;
    pub const MESSAGE_TYPE_MASK: u8 = 0x3C;
}

/// `registered_delivery` request for a delivery receipt (§5.2.17).
pub const REGISTERED_DELIVERY_RECEIPT: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Address {
    pub ton: u8,
    pub npi: u8,
    pub digits: String,
}

impl Address {
    pub const TON_UNKNOWN: u8 = 0;
    pub const NPI_ISDN: u8 = 1;

    pub fn unknown(digits: impl Into<String>) -> Self {
        Self {
            ton: Self::TON_UNKNOWN,
            npi: Self::NPI_ISDN,
            digits: digits.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindKind {
    Receiver,
    Transmitter,
    Transceiver,
}

impl BindKind {
    pub fn request_command_id(self) -> u32 {
        match self {
            Self::Receiver => command_id::BIND_RECEIVER,
            Self::Transmitter => command_id::BIND_TRANSMITTER,
            Self::Transceiver => command_id::BIND_TRANSCEIVER,
        }
    }

    pub fn response_command_id(self) -> u32 {
        match self {
            Self::Receiver => command_id::BIND_RECEIVER_RESP,
            Self::Transmitter => command_id::BIND_TRANSMITTER_RESP,
            Self::Transceiver => command_id::BIND_TRANSCEIVER_RESP,
        }
    }

    pub fn from_request_command_id(id: u32) -> Option<Self> {
        Some(match id {
            command_id::BIND_RECEIVER => Self::Receiver,
            command_id::BIND_TRANSMITTER => Self::Transmitter,
            command_id::BIND_TRANSCEIVER => Self::Transceiver,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bind {
    pub kind: BindKind,
    pub system_id: String,
    pub password: String,
    pub system_type: String,
    pub interface_version: u8,
    pub address_range: Address,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindResp {
    pub kind: BindKind,
    pub system_id: String,
    pub sc_interface_version: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MessageBody {
    pub service_type: String,
    pub source: Address,
    pub destination: Address,
    pub esm_class: u8,
    pub protocol_id: u8,
    pub priority_flag: u8,
    pub schedule_delivery_time: String,
    pub validity_period: String,
    pub registered_delivery: u8,
    pub replace_if_present: u8,
    pub data_coding: u8,
    pub sm_default_msg_id: u8,
    pub text: Vec<u8>,
    /// Original C.S0015 bearer data, carried in a vendor TLV alongside the text.
    pub bearer_data: Option<Vec<u8>>,
    pub teleservice: Option<u16>,
    pub receipted_message_id: Option<String>,
    pub message_state: Option<u8>,
    pub user_message_reference: Option<u16>,
    pub sar_reference: Option<u16>,
    pub sar_total: Option<u8>,
    pub sar_sequence: Option<u8>,
}

impl MessageBody {
    pub fn is_delivery_receipt(&self) -> bool {
        self.esm_class & esm_class::MESSAGE_TYPE_MASK == esm_class::DELIVERY_RECEIPT
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Bind(Bind),
    BindResp(BindResp),
    SubmitSm(Box<MessageBody>),
    SubmitSmResp { message_id: String },
    DeliverSm(Box<MessageBody>),
    DeliverSmResp { message_id: String },
    Unbind,
    UnbindResp,
    EnquireLink,
    EnquireLinkResp,
    GenericNack,
    Unsupported { command_id: u32 },
}

impl Body {
    pub fn command_id(&self) -> u32 {
        match self {
            Body::Bind(bind) => bind.kind.request_command_id(),
            Body::BindResp(resp) => resp.kind.response_command_id(),
            Body::SubmitSm(_) => command_id::SUBMIT_SM,
            Body::SubmitSmResp { .. } => command_id::SUBMIT_SM_RESP,
            Body::DeliverSm(_) => command_id::DELIVER_SM,
            Body::DeliverSmResp { .. } => command_id::DELIVER_SM_RESP,
            Body::Unbind => command_id::UNBIND,
            Body::UnbindResp => command_id::UNBIND_RESP,
            Body::EnquireLink => command_id::ENQUIRE_LINK,
            Body::EnquireLinkResp => command_id::ENQUIRE_LINK_RESP,
            Body::GenericNack => command_id::GENERIC_NACK,
            Body::Unsupported { command_id } => *command_id,
        }
    }

    pub fn is_response(&self) -> bool {
        self.command_id() & 0x8000_0000 != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pdu {
    pub status: CommandStatus,
    pub sequence_number: u32,
    pub body: Body,
}

impl Pdu {
    pub fn request(sequence_number: u32, body: Body) -> Self {
        Self {
            status: CommandStatus::Ok,
            sequence_number,
            body,
        }
    }

    pub fn response(sequence_number: u32, status: CommandStatus, body: Body) -> Self {
        Self {
            status,
            sequence_number,
            body,
        }
    }

    pub fn command_id(&self) -> u32 {
        self.body.command_id()
    }

    pub fn error_for(request: &Pdu, status: CommandStatus) -> Pdu {
        let body = match &request.body {
            Body::Bind(bind) => Body::BindResp(BindResp {
                kind: bind.kind,
                system_id: String::new(),
                sc_interface_version: None,
            }),
            Body::SubmitSm(_) => Body::SubmitSmResp {
                message_id: String::new(),
            },
            Body::DeliverSm(_) => Body::DeliverSmResp {
                message_id: String::new(),
            },
            Body::Unbind => Body::UnbindResp,
            Body::EnquireLink => Body::EnquireLinkResp,
            _ => Body::GenericNack,
        };
        Pdu::response(request.sequence_number, status, body)
    }
}

pub(crate) fn check_payload(body: &MessageBody) -> Result<()> {
    if body.text.len() > u16::MAX as usize {
        return Err(Error::FieldTooLong {
            context: "message text",
            max: u16::MAX as usize,
            actual: body.text.len(),
        });
    }
    Ok(())
}
