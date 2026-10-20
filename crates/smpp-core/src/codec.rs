//! Wire encoding and decoding for SMPP v3.4 PDUs.
//!
//! Definite-length, big-endian, with C-octet strings terminated by NUL. Every
//! decode path is total: a malformed PDU returns an error rather than
//! panicking, because these bytes arrive from the network.

use crate::error::{Error, Result};
use crate::pdu::{
    Address, Bind, BindKind, BindResp, Body, CommandStatus, HEADER_LEN, MAX_PDU_LEN, MessageBody,
    Pdu, command_id, tlv_tag,
};

/// Largest payload `short_message` may carry before the text has to move into
/// the `message_payload` TLV (SMPP v3.4 §5.2.22).
pub const SHORT_MESSAGE_MAX: usize = 254;

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize, context: &'static str) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Truncated {
                context,
                needed: n,
                actual: self.remaining(),
            });
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self, context: &'static str) -> Result<u8> {
        Ok(self.take(1, context)?[0])
    }

    fn u16(&mut self, context: &'static str) -> Result<u16> {
        let b = self.take(2, context)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self, context: &'static str) -> Result<u32> {
        let b = self.take(4, context)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads a NUL-terminated string, rejecting a missing terminator and any
    /// value longer than the field allows.
    fn cstring(&mut self, context: &'static str, max: usize) -> Result<String> {
        let rest = &self.buf[self.pos..];
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(Error::UnterminatedCString(context))?;
        if end > max {
            return Err(Error::FieldTooLong {
                context,
                max,
                actual: end,
            });
        }
        let value = String::from_utf8_lossy(&rest[..end]).into_owned();
        self.pos += end + 1;
        Ok(value)
    }
}

fn put_cstring(out: &mut Vec<u8>, value: &str, context: &'static str, max: usize) -> Result<()> {
    if value.len() > max {
        return Err(Error::FieldTooLong {
            context,
            max,
            actual: value.len(),
        });
    }
    out.extend_from_slice(value.as_bytes());
    out.push(0);
    Ok(())
}

fn put_tlv(out: &mut Vec<u8>, tag: u16, value: &[u8]) {
    out.extend_from_slice(&tag.to_be_bytes());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
}

#[derive(Default)]
struct Tlvs {
    message_payload: Option<Vec<u8>>,
    bearer_data: Option<Vec<u8>>,
    teleservice: Option<u16>,
    receipted_message_id: Option<String>,
    message_state: Option<u8>,
    sc_interface_version: Option<u8>,
    user_message_reference: Option<u16>,
    sar_reference: Option<u16>,
    sar_total: Option<u8>,
    sar_sequence: Option<u8>,
}

fn read_tlvs(reader: &mut Reader<'_>) -> Result<Tlvs> {
    let mut tlvs = Tlvs::default();
    let mut seen = std::collections::HashSet::new();
    while reader.remaining() > 0 {
        let tag = reader.u16("TLV tag")?;
        let len = reader.u16("TLV length")? as usize;
        let value = reader.take(len, "TLV value")?;
        let expected = match tag {
            tlv_tag::USER_MESSAGE_REFERENCE | tlv_tag::SAR_MSG_REF_NUM => {
                Some(std::mem::size_of::<u16>())
            }
            tlv_tag::SAR_TOTAL_SEGMENTS | tlv_tag::SAR_SEGMENT_SEQNUM | tlv_tag::MESSAGE_STATE => {
                Some(std::mem::size_of::<u8>())
            }
            _ => None,
        };
        if expected.is_some_and(|expected| len != expected) || !seen.insert(tag) {
            return Err(Error::InvalidValue {
                context: "optional parameter",
                reason: "invalid length or duplicate tag",
            });
        }
        match tag {
            tlv_tag::MESSAGE_PAYLOAD => tlvs.message_payload = Some(value.to_vec()),
            tlv_tag::IS637_BEARER_DATA => tlvs.bearer_data = Some(value.to_vec()),
            tlv_tag::IS637_TELESERVICE if len == 2 => {
                tlvs.teleservice = Some(u16::from_be_bytes([value[0], value[1]]));
            }
            tlv_tag::RECEIPTED_MESSAGE_ID => {
                let end = value.iter().position(|b| *b == 0).unwrap_or(value.len());
                tlvs.receipted_message_id =
                    Some(String::from_utf8_lossy(&value[..end]).into_owned());
            }
            tlv_tag::MESSAGE_STATE if len == 1 => tlvs.message_state = Some(value[0]),
            tlv_tag::SC_INTERFACE_VERSION if len == 1 => {
                tlvs.sc_interface_version = Some(value[0]);
            }
            tlv_tag::USER_MESSAGE_REFERENCE if len == 2 => {
                tlvs.user_message_reference = Some(u16::from_be_bytes([value[0], value[1]]))
            }
            tlv_tag::SAR_MSG_REF_NUM if len == 2 => {
                tlvs.sar_reference = Some(u16::from_be_bytes([value[0], value[1]]))
            }
            tlv_tag::SAR_TOTAL_SEGMENTS if len == 1 => tlvs.sar_total = Some(value[0]),
            tlv_tag::SAR_SEGMENT_SEQNUM if len == 1 => tlvs.sar_sequence = Some(value[0]),
            _ => {}
        }
    }
    Ok(tlvs)
}

fn decode_address(reader: &mut Reader<'_>, context: &'static str) -> Result<Address> {
    let ton = reader.u8(context)?;
    let npi = reader.u8(context)?;
    let digits = reader.cstring(context, 20)?;
    Ok(Address { ton, npi, digits })
}

fn encode_address(out: &mut Vec<u8>, addr: &Address, context: &'static str) -> Result<()> {
    out.push(addr.ton);
    out.push(addr.npi);
    put_cstring(out, &addr.digits, context, 20)
}

fn decode_message_body(reader: &mut Reader<'_>) -> Result<MessageBody> {
    let service_type = reader.cstring("service_type", 5)?;
    let source = decode_address(reader, "source_addr")?;
    let destination = decode_address(reader, "destination_addr")?;
    let esm_class = reader.u8("esm_class")?;
    let protocol_id = reader.u8("protocol_id")?;
    let priority_flag = reader.u8("priority_flag")?;
    let schedule_delivery_time = reader.cstring("schedule_delivery_time", 16)?;
    let validity_period = reader.cstring("validity_period", 16)?;
    let registered_delivery = reader.u8("registered_delivery")?;
    let replace_if_present = reader.u8("replace_if_present_flag")?;
    let data_coding = reader.u8("data_coding")?;
    let sm_default_msg_id = reader.u8("sm_default_msg_id")?;
    let sm_length = reader.u8("sm_length")? as usize;
    let short_message = reader.take(sm_length, "short_message")?.to_vec();
    let tlvs = read_tlvs(reader)?;

    if !short_message.is_empty() && tlvs.message_payload.is_some() {
        return Err(Error::ConflictingPayload);
    }
    let text = tlvs.message_payload.unwrap_or(short_message);

    let body = MessageBody {
        service_type,
        source,
        destination,
        esm_class,
        protocol_id,
        priority_flag,
        schedule_delivery_time,
        validity_period,
        registered_delivery,
        replace_if_present,
        data_coding,
        sm_default_msg_id,
        text,
        bearer_data: tlvs.bearer_data,
        teleservice: tlvs.teleservice,
        receipted_message_id: tlvs.receipted_message_id,
        message_state: tlvs.message_state,
        user_message_reference: tlvs.user_message_reference,
        sar_reference: tlvs.sar_reference,
        sar_total: tlvs.sar_total,
        sar_sequence: tlvs.sar_sequence,
    };
    validate_sar(&body)?;
    Ok(body)
}

fn validate_sar(body: &MessageBody) -> Result<()> {
    match (body.sar_reference, body.sar_total, body.sar_sequence) {
        (None, None, None) => Ok(()),
        (Some(_), Some(total), Some(sequence))
            if total > 0
                && sequence > 0
                && sequence <= total
                && body.esm_class & crate::esm_class::UDHI == 0 =>
        {
            Ok(())
        }
        _ => Err(Error::InvalidValue {
            context: "SAR",
            reason: "requires all three parameters, valid numbering, and no UDH",
        }),
    }
}

fn encode_message_body(out: &mut Vec<u8>, body: &MessageBody) -> Result<()> {
    validate_sar(body)?;
    crate::pdu::check_payload(body)?;
    put_cstring(out, &body.service_type, "service_type", 5)?;
    encode_address(out, &body.source, "source_addr")?;
    encode_address(out, &body.destination, "destination_addr")?;
    out.push(body.esm_class);
    out.push(body.protocol_id);
    out.push(body.priority_flag);
    put_cstring(
        out,
        &body.schedule_delivery_time,
        "schedule_delivery_time",
        16,
    )?;
    put_cstring(out, &body.validity_period, "validity_period", 16)?;
    out.push(body.registered_delivery);
    out.push(body.replace_if_present);
    out.push(body.data_coding);
    out.push(body.sm_default_msg_id);

    let inline = body.text.len() <= SHORT_MESSAGE_MAX;
    if inline {
        out.push(body.text.len() as u8);
        out.extend_from_slice(&body.text);
    } else {
        out.push(0);
    }

    if !inline {
        put_tlv(out, tlv_tag::MESSAGE_PAYLOAD, &body.text);
    }
    if let Some(bearer) = &body.bearer_data {
        put_tlv(out, tlv_tag::IS637_BEARER_DATA, bearer);
    }
    if let Some(teleservice) = body.teleservice {
        put_tlv(out, tlv_tag::IS637_TELESERVICE, &teleservice.to_be_bytes());
    }
    if let Some(id) = &body.receipted_message_id {
        let mut value = id.as_bytes().to_vec();
        value.push(0);
        put_tlv(out, tlv_tag::RECEIPTED_MESSAGE_ID, &value);
    }
    if let Some(reference) = body.user_message_reference {
        put_tlv(
            out,
            tlv_tag::USER_MESSAGE_REFERENCE,
            &reference.to_be_bytes(),
        );
    }
    if let Some(reference) = body.sar_reference {
        put_tlv(out, tlv_tag::SAR_MSG_REF_NUM, &reference.to_be_bytes());
    }
    if let Some(total) = body.sar_total {
        put_tlv(out, tlv_tag::SAR_TOTAL_SEGMENTS, &[total]);
    }
    if let Some(sequence) = body.sar_sequence {
        put_tlv(out, tlv_tag::SAR_SEGMENT_SEQNUM, &[sequence]);
    }
    if let Some(state) = body.message_state {
        put_tlv(out, tlv_tag::MESSAGE_STATE, &[state]);
    }
    Ok(())
}

pub fn decode(buf: &[u8]) -> Result<Pdu> {
    let mut reader = Reader::new(buf);
    let command_length = reader.u32("command_length")?;
    if (command_length as usize) != buf.len() {
        return Err(Error::InvalidCommandLength(command_length));
    }
    let command_id = reader.u32("command_id")?;
    let status_raw = reader.u32("command_status")?;
    let sequence_number = reader.u32("sequence_number")?;

    let status = CommandStatus::from_u32(status_raw).unwrap_or(CommandStatus::Other(status_raw));

    let body = match command_id {
        command_id::BIND_RECEIVER | command_id::BIND_TRANSMITTER | command_id::BIND_TRANSCEIVER => {
            let kind = BindKind::from_request_command_id(command_id)
                .ok_or(Error::UnknownCommandId(command_id))?;
            let system_id = reader.cstring("system_id", 15)?;
            let password = reader.cstring("password", 8)?;
            let system_type = reader.cstring("system_type", 12)?;
            let interface_version = reader.u8("interface_version")?;
            let address_range = decode_address(&mut reader, "address_range")?;
            Body::Bind(Bind {
                kind,
                system_id,
                password,
                system_type,
                interface_version,
                address_range,
            })
        }
        command_id::BIND_RECEIVER_RESP
        | command_id::BIND_TRANSMITTER_RESP
        | command_id::BIND_TRANSCEIVER_RESP => {
            let kind = BindKind::from_request_command_id(command_id & 0x7FFF_FFFF)
                .ok_or(Error::UnknownCommandId(command_id))?;
            let system_id = if reader.remaining() > 0 {
                reader.cstring("system_id", 15)?
            } else {
                String::new()
            };
            let tlvs = read_tlvs(&mut reader)?;
            Body::BindResp(BindResp {
                kind,
                system_id,
                sc_interface_version: tlvs.sc_interface_version,
            })
        }
        command_id::SUBMIT_SM => Body::SubmitSm(Box::new(decode_message_body(&mut reader)?)),
        command_id::DELIVER_SM => Body::DeliverSm(Box::new(decode_message_body(&mut reader)?)),
        command_id::SUBMIT_SM_RESP => Body::SubmitSmResp {
            message_id: if reader.remaining() > 0 {
                reader.cstring("message_id", 64)?
            } else {
                String::new()
            },
        },
        command_id::DELIVER_SM_RESP => Body::DeliverSmResp {
            message_id: if reader.remaining() > 0 {
                reader.cstring("message_id", 64)?
            } else {
                String::new()
            },
        },
        command_id::UNBIND => Body::Unbind,
        command_id::UNBIND_RESP => Body::UnbindResp,
        command_id::ENQUIRE_LINK => Body::EnquireLink,
        command_id::ENQUIRE_LINK_RESP => Body::EnquireLinkResp,
        command_id::GENERIC_NACK => Body::GenericNack,
        other => Body::Unsupported { command_id: other },
    };

    Ok(Pdu {
        status,
        sequence_number,
        body,
    })
}

pub fn encode(pdu: &Pdu) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    if pdu.status == CommandStatus::Ok || !pdu.body.is_response() {
        match &pdu.body {
            Body::Bind(bind) => {
                put_cstring(&mut body, &bind.system_id, "system_id", 15)?;
                put_cstring(&mut body, &bind.password, "password", 8)?;
                put_cstring(&mut body, &bind.system_type, "system_type", 12)?;
                body.push(bind.interface_version);
                encode_address(&mut body, &bind.address_range, "address_range")?;
            }
            Body::BindResp(resp) => {
                put_cstring(&mut body, &resp.system_id, "system_id", 15)?;
                if let Some(version) = resp.sc_interface_version {
                    put_tlv(&mut body, tlv_tag::SC_INTERFACE_VERSION, &[version]);
                }
            }
            Body::SubmitSm(msg) | Body::DeliverSm(msg) => encode_message_body(&mut body, msg)?,
            Body::SubmitSmResp { message_id } | Body::DeliverSmResp { message_id } => {
                put_cstring(&mut body, message_id, "message_id", 64)?;
            }
            Body::Unbind | Body::UnbindResp | Body::EnquireLink | Body::EnquireLinkResp => {}
            Body::GenericNack => {}
            Body::Unsupported { command_id } => return Err(Error::UnknownCommandId(*command_id)),
        }
    }

    let total = HEADER_LEN + body.len();
    if total as u32 > MAX_PDU_LEN {
        return Err(Error::InvalidCommandLength(total as u32));
    }

    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(total as u32).to_be_bytes());
    out.extend_from_slice(&pdu.command_id().to_be_bytes());
    out.extend_from_slice(&pdu.status.as_u32().to_be_bytes());
    out.extend_from_slice(&pdu.sequence_number.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_frame(buf: &[u8]) -> Result<Option<(Pdu, usize)>> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    let command_length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if command_length < HEADER_LEN as u32 || command_length > MAX_PDU_LEN {
        return Err(Error::InvalidCommandLength(command_length));
    }
    let len = command_length as usize;
    if buf.len() < len {
        return Ok(None);
    }
    Ok(Some((decode(&buf[..len])?, len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdu::{INTERFACE_VERSION_34, REGISTERED_DELIVERY_RECEIPT, esm_class};

    fn round_trip(pdu: &Pdu) {
        let bytes = encode(pdu).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(&decoded, pdu, "decoded PDU differs");
        let reencoded = encode(&decoded).expect("re-encode");
        assert_eq!(reencoded, bytes, "re-encode is not byte-identical");
    }

    fn message(text: &[u8]) -> MessageBody {
        MessageBody {
            service_type: String::new(),
            source: Address::unknown("7025550100"),
            destination: Address::unknown("4155550118"),
            esm_class: 0,
            protocol_id: 0,
            priority_flag: 0,
            schedule_delivery_time: String::new(),
            validity_period: String::new(),
            registered_delivery: REGISTERED_DELIVERY_RECEIPT,
            replace_if_present: 0,
            data_coding: 0,
            sm_default_msg_id: 0,
            text: text.to_vec(),
            bearer_data: Some(vec![0x00, 0x03, 0x10, 0x02, 0x10]),
            teleservice: Some(4098),
            receipted_message_id: None,
            message_state: None,
            ..MessageBody::default()
        }
    }

    #[test]
    fn bind_transceiver_round_trips() {
        round_trip(&Pdu::request(
            1,
            Body::Bind(Bind {
                kind: BindKind::Transceiver,
                system_id: "kw6xyz-lv".to_string(),
                password: "s3cret".to_string(),
                system_type: String::new(),
                interface_version: INTERFACE_VERSION_34,
                address_range: Address::default(),
            }),
        ));
    }

    #[test]
    fn bind_resp_round_trips_with_interface_version() {
        round_trip(&Pdu::response(
            1,
            CommandStatus::Ok,
            Body::BindResp(BindResp {
                kind: BindKind::Transceiver,
                system_id: "1xbts-hub".to_string(),
                sc_interface_version: Some(INTERFACE_VERSION_34),
            }),
        ));
    }

    #[test]
    fn submit_sm_round_trips_with_inline_text() {
        round_trip(&Pdu::request(
            7,
            Body::SubmitSm(Box::new(message(b"on my way"))),
        ));
    }

    #[test]
    fn deliver_sm_round_trips_with_long_text_in_payload_tlv() {
        let long = vec![b'x'; SHORT_MESSAGE_MAX + 40];
        round_trip(&Pdu::request(8, Body::DeliverSm(Box::new(message(&long)))));
    }

    #[test]
    fn long_text_moves_out_of_short_message() {
        let long = vec![b'y'; SHORT_MESSAGE_MAX + 1];
        let bytes = encode(&Pdu::request(1, Body::SubmitSm(Box::new(message(&long))))).unwrap();
        let decoded = decode(&bytes).unwrap();
        let Body::SubmitSm(body) = decoded.body else {
            panic!("expected submit_sm");
        };
        assert_eq!(body.text.len(), SHORT_MESSAGE_MAX + 1);
    }

    #[test]
    fn bearer_data_survives_verbatim() {
        let bearer: Vec<u8> = (0u8..=255).collect();
        let mut msg = message(b"text");
        msg.bearer_data = Some(bearer.clone());
        let bytes = encode(&Pdu::request(3, Body::DeliverSm(Box::new(msg)))).unwrap();
        let decoded = decode(&bytes).unwrap();
        let Body::DeliverSm(body) = decoded.body else {
            panic!("expected deliver_sm");
        };
        assert_eq!(body.bearer_data, Some(bearer));
    }

    #[test]
    fn delivery_receipt_is_recognized() {
        let mut msg = message(b"id:abc stat:DELIVRD");
        msg.esm_class = esm_class::DELIVERY_RECEIPT;
        msg.receipted_message_id = Some("abc123".to_string());
        msg.message_state = Some(2);
        assert!(msg.is_delivery_receipt());
        round_trip(&Pdu::request(9, Body::DeliverSm(Box::new(msg))));
    }

    #[test]
    fn simple_pdus_round_trip() {
        round_trip(&Pdu::request(11, Body::EnquireLink));
        round_trip(&Pdu::response(11, CommandStatus::Ok, Body::EnquireLinkResp));
        round_trip(&Pdu::request(12, Body::Unbind));
        round_trip(&Pdu::response(12, CommandStatus::Ok, Body::UnbindResp));
        round_trip(&Pdu::response(
            13,
            CommandStatus::InvalidCommandId,
            Body::GenericNack,
        ));
    }

    #[test]
    fn error_response_matches_the_request_it_answers() {
        let request = Pdu::request(5, Body::SubmitSm(Box::new(message(b"hi"))));
        let error = Pdu::error_for(&request, CommandStatus::InvalidDestAddress);
        assert_eq!(error.sequence_number, 5);
        assert_eq!(error.status, CommandStatus::InvalidDestAddress);
        assert!(matches!(error.body, Body::SubmitSmResp { .. }));
    }

    #[test]
    fn both_payload_fields_populated_is_rejected() {
        let mut bytes = encode(&Pdu::request(
            1,
            Body::SubmitSm(Box::new(message(b"inline"))),
        ))
        .unwrap();
        let extra = [0x04u8, 0x24, 0x00, 0x02, b'h', b'i'];
        bytes.extend_from_slice(&extra);
        let new_len = bytes.len() as u32;
        bytes[0..4].copy_from_slice(&new_len.to_be_bytes());
        assert_eq!(decode(&bytes), Err(Error::ConflictingPayload));
    }

    #[test]
    fn truncated_and_malformed_input_never_panics() {
        let full = encode(&Pdu::request(1, Body::SubmitSm(Box::new(message(b"x"))))).unwrap();
        for cut in 0..full.len() {
            let _ = decode(&full[..cut]);
            let _ = decode_frame(&full[..cut]);
        }
        let _ = decode(&[0xFF; 16]);
        let _ = decode(&[]);
        let mut unterminated = full.clone();
        for byte in unterminated.iter_mut().skip(HEADER_LEN) {
            if *byte == 0 {
                *byte = 0x41;
            }
        }
        let _ = decode(&unterminated);
    }

    #[test]
    fn frame_decoding_waits_for_a_whole_pdu() {
        let a = encode(&Pdu::request(1, Body::EnquireLink)).unwrap();
        let b = encode(&Pdu::request(2, Body::Unbind)).unwrap();
        let mut stream = Vec::new();
        stream.extend_from_slice(&a);
        stream.extend_from_slice(&b);

        assert_eq!(decode_frame(&stream[..4]).unwrap(), None);
        let (first, used) = decode_frame(&stream).unwrap().expect("first PDU");
        assert_eq!(used, a.len());
        assert_eq!(first.sequence_number, 1);
        let (second, used) = decode_frame(&stream[used..]).unwrap().expect("second PDU");
        assert_eq!(used, b.len());
        assert_eq!(second.sequence_number, 2);
    }

    #[test]
    fn oversized_command_length_is_rejected_before_allocation() {
        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(&(MAX_PDU_LEN + 1).to_be_bytes());
        assert!(matches!(
            decode_frame(&header),
            Err(Error::InvalidCommandLength(_))
        ));
    }
}

#[cfg(test)]
mod multipart_tests {
    use super::*;

    #[test]
    fn sar_and_unicode_survive_the_wire_and_invalid_combinations_fail() {
        let (data_coding, text) = crate::text::encode("Привет 世界").unwrap();
        let mut body = MessageBody {
            source: Address::unknown("1000"),
            destination: Address::unknown("2000"),
            data_coding,
            text,
            user_message_reference: Some(65535),
            sar_reference: Some(1234),
            sar_total: Some(2),
            sar_sequence: Some(1),
            ..Default::default()
        };
        let pdu = Pdu::request(42, Body::SubmitSm(Box::new(body.clone())));
        assert_eq!(decode(&encode(&pdu).unwrap()).unwrap(), pdu);
        body.esm_class = crate::esm_class::UDHI;
        assert!(encode(&Pdu::request(42, Body::SubmitSm(Box::new(body.clone())))).is_err());
        body.esm_class = 0;
        body.sar_sequence = Some(3);
        assert!(encode(&Pdu::request(42, Body::SubmitSm(Box::new(body.clone())))).is_err());
        body.sar_sequence = None;
        assert!(encode(&Pdu::request(42, Body::SubmitSm(Box::new(body)))).is_err());
    }

    #[test]
    fn malformed_optional_parameters_are_rejected_instead_of_silently_ignored() {
        for bytes in [
            vec![0x02, 0x04, 0, 1, 0],
            vec![0x02, 0x04, 0, 2, 0, 1, 0x02, 0x04, 0, 2, 0, 1],
        ] {
            assert!(read_tlvs(&mut Reader::new(&bytes)).is_err());
        }
    }
}
