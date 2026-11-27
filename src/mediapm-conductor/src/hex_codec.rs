//! Minimal hex codec for conductor-owned digest and secret strings.

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Encodes bytes as lowercase hex.
#[must_use]
pub(crate) fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Decodes an even-length hex string.
///
/// # Errors
///
/// Returns an error for an odd length or a non-hex character.
pub(crate) fn decode(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex input must have an even length".to_owned());
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().as_chunks::<2>().0 {
        let high = nibble(pair[0])?;
        let low = nibble(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

/// Decodes one hex digit.
///
/// # Errors
///
/// Returns an error when `byte` is not an ASCII hex digit.
fn nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("hex input contains a non-hex character".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn encode_renders_lowercase_hex() {
        assert_eq!(encode(&[0x00, 0x0f, 0x10, 0xab]), "000f10ab");
    }

    #[test]
    fn decode_accepts_both_hex_cases() {
        assert_eq!(
            decode("000f10Ab").expect("mixed-case hex decodes"),
            vec![0x00, 0x0f, 0x10, 0xab]
        );
    }

    #[test]
    fn decode_rejects_odd_length() {
        assert!(decode("abc").is_err());
    }

    #[test]
    fn decode_rejects_non_hex_characters() {
        assert!(decode("zz").is_err());
    }
}
