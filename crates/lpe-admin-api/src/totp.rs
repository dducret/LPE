use sha2::{Digest, Sha256};
use uuid::Uuid;

const TOTP_STEP_SECONDS: u64 = 30;
const TOTP_DIGITS: u32 = 6;
const BASE32_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

pub fn generate_secret() -> String {
    encode_base32(Uuid::new_v4().as_bytes())
}

pub fn unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub fn verify_code(secret: &str, code: &str, unix_time: u64) -> bool {
    let candidate = code.trim();
    if candidate.len() != TOTP_DIGITS as usize || !candidate.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }

    (-1i64..=1).any(|offset| {
        let ts = if offset.is_negative() {
            unix_time.saturating_sub(offset.unsigned_abs() * TOTP_STEP_SECONDS)
        } else {
            unix_time.saturating_add(offset as u64 * TOTP_STEP_SECONDS)
        };
        generate_code(secret, ts)
            .map(|expected| expected == candidate)
            .unwrap_or(false)
    })
}

pub fn otpauth_url(hostname: &str, email: &str, label: &str, secret: &str) -> String {
    let issuer = url_encode(hostname.trim());
    let account_label = url_encode(&format!("{} ({})", label.trim(), email.trim().to_lowercase()));
    format!(
        "otpauth://totp/{issuer}:{account_label}?secret={secret}&issuer={issuer}&algorithm=SHA256&digits={TOTP_DIGITS}&period={TOTP_STEP_SECONDS}"
    )
}

fn generate_code(secret: &str, unix_time: u64) -> Option<String> {
    let secret = decode_base32(secret)?;
    let counter = unix_time / TOTP_STEP_SECONDS;
    let hmac = hmac_sha256(&secret, &counter.to_be_bytes());
    let offset = (hmac.last().copied()? & 0x0f) as usize;
    let slice = hmac.get(offset..offset + 4)?;
    let value = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]) & 0x7fff_ffff;
    Some(format!(
        "{:0width$}",
        value % 10_u32.pow(TOTP_DIGITS),
        width = TOTP_DIGITS as usize
    ))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut normalized_key = [0u8; 64];
    if key.len() > 64 {
        let digest = Sha256::digest(key);
        normalized_key[..32].copy_from_slice(&digest);
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0u8; 64];
    let mut outer_pad = [0u8; 64];
    for (idx, value) in normalized_key.iter().enumerate() {
        inner_pad[idx] = value ^ 0x36;
        outer_pad[idx] = value ^ 0x5c;
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(data);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    outer.finalize().into()
}

fn encode_base32(bytes: &[u8]) -> String {
    let mut output = String::new();
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in bytes {
        buffer = (buffer << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let index = ((buffer >> bits) & 0x1f) as usize;
            output.push(BASE32_ALPHABET[index] as char);
        }
    }
    if bits > 0 {
        let index = ((buffer << (5 - bits)) & 0x1f) as usize;
        output.push(BASE32_ALPHABET[index] as char);
    }
    output
}

fn decode_base32(input: &str) -> Option<Vec<u8>> {
    let mut buffer = 0u32;
    let mut bits = 0u8;
    let mut output = Vec::new();
    for character in input.chars().filter(|ch| !ch.is_ascii_whitespace()) {
        let value = match character.to_ascii_uppercase() {
            'A'..='Z' => character.to_ascii_uppercase() as u8 - b'A',
            '2'..='7' => (character as u8) - b'2' + 26,
            '=' => continue,
            _ => return None,
        };
        buffer = (buffer << 5) | u32::from(value);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            output.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Some(output)
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            b' ' => "%20".to_string(),
            _ => format!("%{:02X}", byte),
        })
        .collect::<Vec<_>>()
        .join("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_secret_is_base32() {
        let secret = generate_secret();
        assert!(!secret.is_empty());
        assert!(secret.chars().all(|ch| ch.is_ascii_uppercase() || ('2'..='7').contains(&ch)));
    }

    #[test]
    fn current_code_verifies() {
        let secret = encode_base32(b"0123456789abcdef");
        let code = generate_code(&secret, 1_715_000_000).unwrap();
        assert!(verify_code(&secret, &code, 1_715_000_000));
    }

    #[test]
    fn otp_url_contains_expected_parameters() {
        let url = otpauth_url("mail.example.test", "admin@example.test", "Admin", "ABC123");
        assert!(url.contains("algorithm=SHA256"));
        assert!(url.contains("secret=ABC123"));
    }
}
