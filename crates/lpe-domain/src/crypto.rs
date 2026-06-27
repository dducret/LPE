use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub fn hex_lower(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(Sha256::digest(bytes))
}

pub fn sha256_hex_prefix(bytes: &[u8], hex_chars: usize) -> String {
    sha256_hex(bytes).chars().take(hex_chars).collect()
}

pub fn hmac_sha256(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

pub fn hmac_sha256_hex(key: &[u8], payload: &[u8]) -> String {
    hex_lower(hmac_sha256(key, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_lowercase() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hmac_sha256_hex_matches_rfc_4231_case() {
        assert_eq!(
            hmac_sha256_hex(&[0x0b; 20], b"Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }
}
