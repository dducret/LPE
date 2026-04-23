use anyhow::{Context, Result};
use email_auth::dkim::{CanonicalizationMethod, DkimSigner};
use lpe_domain::OutboundMessageHandoffRequest;
use std::fs;

#[derive(Debug, Clone)]
pub(crate) struct DkimSigningOutcome {
    pub message: Vec<u8>,
    pub detail: String,
    pub signed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DkimKeyConfig {
    pub domain: String,
    pub selector: String,
    pub key_path: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DkimConfig {
    pub enabled: bool,
    pub headers: Vec<String>,
    pub over_sign: bool,
    pub expiration_seconds: Option<u64>,
    pub keys: Vec<DkimKeyConfig>,
}

pub(crate) fn maybe_sign_outbound_message(
    config: &DkimConfig,
    payload: &OutboundMessageHandoffRequest,
    raw_message: &[u8],
) -> Result<DkimSigningOutcome> {
    if !config.enabled {
        return Ok(DkimSigningOutcome {
            message: raw_message.to_vec(),
            detail: "outbound DKIM signing disabled by configuration".to_string(),
            signed: false,
        });
    }

    let sender_domain = sender_domain(payload);
    let Some(key) = config
        .keys
        .iter()
        .find(|entry| entry.domain.eq_ignore_ascii_case(&sender_domain))
    else {
        return Ok(DkimSigningOutcome {
            message: raw_message.to_vec(),
            detail: format!("no DKIM key configured for sender domain {sender_domain}"),
            signed: false,
        });
    };

    let private_key = fs::read(&key.key_path)
        .with_context(|| format!("unable to read DKIM private key {}", key.key_path))?;
    let headers = parse_headers(raw_message);
    let body = split_body(raw_message);
    let mut signer = DkimSigner::rsa_sha256(&key.domain, &key.selector, &private_key)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .header_canonicalization(CanonicalizationMethod::Relaxed)
        .body_canonicalization(CanonicalizationMethod::Relaxed)
        .headers(config.headers.clone())
        .over_sign(config.over_sign);
    if let Some(expiration_seconds) = config.expiration_seconds {
        signer = signer.expiration(expiration_seconds);
    }
    let header_refs = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let signature = signer
        .sign_message(&header_refs, &body)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let mut signed = Vec::with_capacity(raw_message.len() + signature.len() + 20);
    signed.extend_from_slice(format!("DKIM-Signature: {signature}\r\n").as_bytes());
    signed.extend_from_slice(raw_message);
    Ok(DkimSigningOutcome {
        message: signed,
        detail: format!(
            "applied outbound DKIM signature for domain {} with selector {}",
            key.domain, key.selector
        ),
        signed: true,
    })
}

fn parse_headers(message: &[u8]) -> Vec<(String, String)> {
    let header_bytes = message
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| &message[..index])
        .unwrap_or(message);
    let text = String::from_utf8_lossy(header_bytes);
    let mut headers = Vec::new();
    let mut current_name = String::new();
    let mut current_value = String::new();
    for line in text.split("\r\n") {
        if line.starts_with(' ') || line.starts_with('\t') {
            current_value.push_str("\r\n");
            current_value.push_str(line);
            continue;
        }
        if !current_name.is_empty() {
            headers.push((current_name.clone(), current_value.clone()));
        }
        if let Some((name, value)) = line.split_once(':') {
            current_name = name.to_string();
            current_value = value.to_string();
        } else {
            current_name.clear();
            current_value.clear();
        }
    }
    if !current_name.is_empty() {
        headers.push((current_name, current_value));
    }
    headers
}

fn split_body(message: &[u8]) -> Vec<u8> {
    message
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| message[index + 4..].to_vec())
        .unwrap_or_default()
}

fn sender_domain(payload: &OutboundMessageHandoffRequest) -> String {
    payload
        .sender_address
        .as_deref()
        .unwrap_or(&payload.from_address)
        .trim()
        .trim_matches(['<', '>'])
        .rsplit_once('@')
        .map(|(_, domain)| domain.to_ascii_lowercase())
        .unwrap_or_else(|| "invalid".to_string())
}

#[cfg(test)]
mod tests {
    use super::{maybe_sign_outbound_message, DkimConfig, DkimKeyConfig};
    use crate::env_test_lock;
    use lpe_domain::{OutboundMessageHandoffRequest, TransportRecipient};
    use uuid::Uuid;

    fn payload() -> OutboundMessageHandoffRequest {
        OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: Some("Dest".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Signed".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: Some("<signed@example.test>".to_string()),
            attempt_count: 0,
            last_attempt_error: None,
        }
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn dkim_signer_adds_header_when_domain_key_exists() {
        let _guard = env_test_lock();
        let config = DkimConfig {
            enabled: true,
            headers: vec![
                "from".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "subject".to_string(),
                "mime-version".to_string(),
                "content-type".to_string(),
                "message-id".to_string(),
            ],
            over_sign: true,
            expiration_seconds: None,
            keys: vec![DkimKeyConfig {
                domain: "example.test".to_string(),
                selector: "mta".to_string(),
                key_path: "tests/fixtures/rsa2048.pem".to_string(),
            }],
        };
        let raw = concat!(
            "From: Sender <sender@example.test>\r\n",
            "To: Dest <dest@example.test>\r\n",
            "Subject: Signed\r\n",
            "Message-Id: <signed@example.test>\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Body\r\n"
        )
        .as_bytes()
        .to_vec();

        let outcome = maybe_sign_outbound_message(&config, &payload(), &raw).unwrap();

        assert!(outcome.signed);
        let signed = String::from_utf8(outcome.message).unwrap();
        assert!(signed.starts_with("DKIM-Signature: "));
        assert!(signed.contains(" d=example.test;"));
        assert!(signed.contains(" s=mta;"));
    }
}
