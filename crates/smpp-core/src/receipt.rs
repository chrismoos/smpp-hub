use crate::{MessageBody, pdu::message_state, text};

const MAX_MESSAGE_ID_OCTETS: usize = 64;

#[derive(Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub message_id: String,
    pub state: u8,
}

pub fn parse(body: &MessageBody) -> Option<DeliveryReceipt> {
    if !body.is_delivery_receipt() {
        return None;
    }
    let receipt = match (&body.receipted_message_id, body.message_state) {
        (Some(id), Some(state)) => DeliveryReceipt {
            message_id: id.clone(),
            state,
        },
        (None, None) => parse_text(&text::decode(body).ok()?)?,
        _ => return None,
    };
    if receipt.message_id.is_empty()
        || receipt.message_id.len() > MAX_MESSAGE_ID_OCTETS
        || receipt.message_id.contains('\0')
        || !(message_state::ENROUTE..=message_state::REJECTED).contains(&receipt.state)
    {
        return None;
    }
    Some(receipt)
}

// SMPP v3.4 Appendix B describes receipts carried in short_message.
fn parse_text(value: &str) -> Option<DeliveryReceipt> {
    let mut id = None;
    let mut state = None;
    for field in value.split_whitespace() {
        if field.starts_with("text:") {
            break;
        }
        if let Some(value) = field.strip_prefix("id:")
            && id.replace(value.to_owned()).is_some()
        {
            return None;
        }
        if let Some(value) = field.strip_prefix("stat:") {
            let parsed = match value {
                "ENROUTE" => message_state::ENROUTE,
                "DELIVRD" => message_state::DELIVERED,
                "EXPIRED" => message_state::EXPIRED,
                "DELETED" => message_state::DELETED,
                "UNDELIV" => message_state::UNDELIVERABLE,
                "ACCEPTD" => message_state::ACCEPTED,
                "UNKNOWN" => message_state::UNKNOWN,
                "REJECTD" => message_state::REJECTED,
                _ => return None,
            };
            if state.replace(parsed).is_some() {
                return None;
            }
        }
    }
    Some(DeliveryReceipt {
        message_id: id?,
        state: state?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(value: &str) -> MessageBody {
        MessageBody {
            esm_class: crate::esm_class::DELIVERY_RECEIPT,
            data_coding: text::IA5,
            text: value.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn standard_text_states_are_recognized() {
        for (name, state) in [
            ("ENROUTE", message_state::ENROUTE),
            ("DELIVRD", message_state::DELIVERED),
            ("EXPIRED", message_state::EXPIRED),
            ("DELETED", message_state::DELETED),
            ("UNDELIV", message_state::UNDELIVERABLE),
            ("ACCEPTD", message_state::ACCEPTED),
            ("UNKNOWN", message_state::UNKNOWN),
            ("REJECTD", message_state::REJECTED),
        ] {
            assert_eq!(
                parse(&body(&format!(
                    "id:abc sub:001 stat:{name} err:000 text:hello"
                ))),
                Some(DeliveryReceipt {
                    message_id: "abc".into(),
                    state
                })
            );
        }
    }

    #[test]
    fn user_text_cannot_supply_receipt_fields() {
        for invalid in [
            "text:hello id:abc stat:DELIVRD",
            "id:abc text:hello stat:DELIVRD",
            "id:abc id:other stat:DELIVRD",
            "id:abc stat:DELIVRD stat:EXPIRED",
            "id: stat:DELIVRD",
            "id:abc stat:UNRECOGNIZED",
        ] {
            assert_eq!(parse(&body(invalid)), None, "{invalid}");
        }
    }

    #[test]
    fn complete_tlvs_take_precedence_and_partial_tlvs_are_rejected() {
        let mut message = body("id:text-id stat:EXPIRED");
        message.receipted_message_id = Some("tlv-id".into());
        assert_eq!(parse(&message), None);
        message.message_state = Some(message_state::DELIVERED);
        assert_eq!(
            parse(&message),
            Some(DeliveryReceipt {
                message_id: "tlv-id".into(),
                state: message_state::DELIVERED
            })
        );
        message.esm_class = MessageBody::default().esm_class;
        assert_eq!(parse(&message), None);
    }
}
