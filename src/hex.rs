//! Lowercase hex encoding for digests and random tokens.

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Encode `bytes` as a lowercase hex string.
pub fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX_DIGITS[(b >> 4) as usize] as char);
        out.push(HEX_DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::encode;

    #[test]
    fn encodes_lowercase() {
        assert_eq!(encode(&[]), "");
        assert_eq!(encode(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }
}
