use crate::{Error, MessageBody, Result};

pub const SMSC_DEFAULT: u8 = 0;
pub const IA5: u8 = 1;
pub const LATIN1: u8 = 3;
pub const UCS2: u8 = 8;
const UCS2_CODE_UNIT_OCTETS: usize = std::mem::size_of::<u16>();
const UCS2_SURROGATE_START: u16 = 0xD800;
const UCS2_SURROGATE_END: u16 = 0xDFFF;
const GSM_ESCAPE: u8 = 0x1B;
const GSM_ALPHABET: &str = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\u{001b}ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà";

fn invalid(reason: &'static str) -> Error {
    Error::InvalidValue {
        context: "SMPP text",
        reason,
    }
}

pub fn encode(text: &str) -> Result<(u8, Vec<u8>)> {
    if text
        .chars()
        .any(|character| u32::from(character) > u32::from(u16::MAX))
    {
        return Err(invalid("characters outside UCS2 are unsupported"));
    }
    if text.is_ascii() {
        Ok((IA5, text.as_bytes().to_vec()))
    } else {
        Ok((
            UCS2,
            text.encode_utf16().flat_map(u16::to_be_bytes).collect(),
        ))
    }
}

pub fn decode(body: &MessageBody) -> Result<String> {
    decode_bytes(body.data_coding, &body.text)
}

pub fn decode_bytes(coding: u8, bytes: &[u8]) -> Result<String> {
    match coding {
        IA5 if bytes.is_ascii() => {
            String::from_utf8(bytes.to_vec()).map_err(|_| invalid("invalid IA5"))
        }
        LATIN1 => Ok(bytes.iter().map(|b| char::from(*b)).collect()),
        UCS2 if bytes.len().is_multiple_of(UCS2_CODE_UNIT_OCTETS)
            && !bytes
                .as_chunks::<UCS2_CODE_UNIT_OCTETS>()
                .0
                .iter()
                .any(|pair| {
                    (UCS2_SURROGATE_START..=UCS2_SURROGATE_END)
                        .contains(&u16::from_be_bytes([pair[0], pair[1]]))
                }) =>
        {
            String::from_utf16(
                &bytes
                    .as_chunks::<UCS2_CODE_UNIT_OCTETS>()
                    .0
                    .iter()
                    .map(|p| u16::from_be_bytes([p[0], p[1]]))
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| invalid("invalid UCS2"))
        }
        SMSC_DEFAULT => {
            let alphabet: Vec<char> = GSM_ALPHABET.chars().collect();
            let mut out = String::new();
            let mut iter = bytes.iter();
            while let Some(&byte) = iter.next() {
                if byte == GSM_ESCAPE {
                    out.push(match iter.next() {
                        Some(0x0A) => '\u{000c}',
                        Some(0x14) => '^',
                        Some(0x28) => '{',
                        Some(0x29) => '}',
                        Some(0x2F) => '\\',
                        Some(0x3C) => '[',
                        Some(0x3D) => '~',
                        Some(0x3E) => ']',
                        Some(0x40) => '|',
                        Some(0x65) => '€',
                        _ => return Err(invalid("invalid GSM extension")),
                    });
                } else {
                    out.push(
                        *alphabet
                            .get(usize::from(byte))
                            .ok_or(invalid("invalid GSM character"))?,
                    );
                }
            }
            Ok(out)
        }
        _ => Err(invalid("unsupported or invalid data_coding")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_and_ascii_round_trip_without_replacement() {
        for text in ["plain ASCII @", "Привет 世界"] {
            let (coding, bytes) = encode(text).unwrap();
            assert_eq!(decode_bytes(coding, &bytes).unwrap(), text);
        }
        assert!(encode("Hello 😀").is_err());
        assert!(decode_bytes(UCS2, &[0xD8, 0]).is_err());
        assert!(decode_bytes(IA5, &[0xFF]).is_err());
    }
    #[test]
    fn default_alphabet_and_extensions_are_not_utf8() {
        assert_eq!(
            decode_bytes(SMSC_DEFAULT, &[0, 1, GSM_ESCAPE, 0x65]).unwrap(),
            "@£€"
        );
    }
}
