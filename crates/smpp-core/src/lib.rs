#![doc = include_str!("../README.md")]

pub mod codec;
pub mod connection;
pub mod error;
pub mod pdu;
pub mod receipt;
pub mod session;
pub mod text;
pub mod transport;

pub use codec::{SHORT_MESSAGE_MAX, decode, decode_frame, encode};
pub use error::{Error, Result};
pub use pdu::{
    Address, Bind, BindKind, BindResp, Body, CommandStatus, MessageBody, Pdu,
    REGISTERED_DELIVERY_RECEIPT, command_id, esm_class, tlv_tag,
};
pub use session::{Action, Config, Role, Session, State};
pub use transport::{PduReader, PduWriter};

pub use connection::{Connection, ConnectionOptions};

pub mod tcp;
