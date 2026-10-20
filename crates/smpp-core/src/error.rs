use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidCommandLength(u32),
    UnknownCommandId(u32),
    Truncated {
        context: &'static str,
        needed: usize,
        actual: usize,
    },
    /// A C-octet string ran to the end of the buffer without its NUL.
    UnterminatedCString(&'static str),
    FieldTooLong {
        context: &'static str,
        max: usize,
        actual: usize,
    },
    InvalidValue {
        context: &'static str,
        reason: &'static str,
    },
    ConflictingPayload,
    UnexpectedResponse(u32),
    InvalidSessionState {
        operation: &'static str,
        state: &'static str,
    },
    WindowFull,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidCommandLength(len) => {
                write!(f, "invalid SMPP command_length {len}")
            }
            Error::UnknownCommandId(id) => write!(f, "unknown SMPP command_id 0x{id:08x}"),
            Error::Truncated {
                context,
                needed,
                actual,
            } => write!(f, "{context} truncated: need {needed}, have {actual}"),
            Error::UnterminatedCString(context) => {
                write!(f, "{context} is not NUL-terminated")
            }
            Error::FieldTooLong {
                context,
                max,
                actual,
            } => write!(f, "{context} is {actual} octets, maximum {max}"),
            Error::InvalidValue { context, reason } => write!(f, "{context}: {reason}"),
            Error::ConflictingPayload => {
                f.write_str("short_message and message_payload are mutually exclusive")
            }
            Error::UnexpectedResponse(seq) => {
                write!(f, "response for unknown sequence_number {seq}")
            }
            Error::InvalidSessionState { operation, state } => {
                write!(f, "cannot {operation} while session is {state}")
            }
            Error::WindowFull => f.write_str("outstanding request window is full"),
        }
    }
}

impl std::error::Error for Error {}
