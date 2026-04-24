use anyhow::Result;
use lpe_domain::{
    SignedIntegrationHeaders, INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER,
    INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::env;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::{integration_shared_secret, storage};

pub(crate) const RECIPIENT_VERIFICATION_PATH: &str = "/internal/lpe-ct/recipient-verification";

static RECIPIENT_VERIFICATION_CACHE: OnceLock<Mutex<BTreeMap<String, CachedRecipientVerdict>>> =
    OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddressRole {
    Sender,
    Recipient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddressPolicyVerdict {
    Allow,
    Reject(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecipientVerificationVerdict {
    Accept,
    Reject(String),
    Defer(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttachmentPolicyVerdict {
    Accept,
    Restrict(String),
}

#[derive(Debug, Clone, Serialize)]
struct RecipientVerificationRequest {
    trace_id: String,
    direction: String,
    sender: Option<String>,
    recipient: String,
    helo: Option<String>,
    peer: Option<String>,
    account_id: Option<Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecipientVerificationResponse {
    verified: bool,
    detail: Option<String>,
    cache_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
struct CachedRecipientVerdict {
    verdict: RecipientVerificationVerdict,
    expires_at_unix: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AddressPolicyConfig {
    pub allow_senders: Vec<String>,
    pub block_senders: Vec<String>,
    pub allow_recipients: Vec<String>,
    pub block_recipients: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AttachmentPolicyConfig {
    pub allow_extensions: Vec<String>,
    pub block_extensions: Vec<String>,
    pub allow_mime_types: Vec<String>,
    pub block_mime_types: Vec<String>,
    pub allow_detected_types: Vec<String>,
    pub block_detected_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecipientVerificationConfig {
    pub enabled: bool,
    pub fail_closed: bool,
    pub cache_ttl_seconds: u64,
    pub local_db: storage::LocalDbConfig,
}

pub(crate) fn evaluate_address_policy_with_config(
    config: &AddressPolicyConfig,
    role: AddressRole,
    address: &str,
) -> AddressPolicyVerdict {
    let normalized = normalize_address(address);
    if normalized.is_empty() {
        return AddressPolicyVerdict::Reject("address is empty".to_string());
    }

    let (allow_list, block_list, noun) = match role {
        AddressRole::Sender => (&config.allow_senders, &config.block_senders, "sender"),
        AddressRole::Recipient => (
            &config.allow_recipients,
            &config.block_recipients,
            "recipient",
        ),
    };

    if let Some(entry) = match_address_rule(block_list, &normalized) {
        return AddressPolicyVerdict::Reject(format!(
            "{noun} {normalized} matched block list entry {entry}"
        ));
    }
    if !allow_list.is_empty() && match_address_rule(allow_list, &normalized).is_none() {
        return AddressPolicyVerdict::Reject(format!(
            "{noun} {normalized} is not present in the configured allow list"
        ));
    }

    AddressPolicyVerdict::Allow
}

#[cfg(test)]
pub(crate) fn evaluate_address_policy(role: AddressRole, address: &str) -> AddressPolicyVerdict {
    let config = address_policy_config_from_env();
    evaluate_address_policy_with_config(&config, role, address)
}

pub(crate) async fn verify_recipient_with_core(
    client: &reqwest::Client,
    config: &RecipientVerificationConfig,
    core_base_url: &str,
    sender: Option<&str>,
    recipient: &str,
    helo: Option<&str>,
    peer: Option<&str>,
    account_id: Option<Uuid>,
) -> RecipientVerificationVerdict {
    if !config.enabled {
        return RecipientVerificationVerdict::Accept;
    }

    let normalized_recipient = normalize_address(recipient);
    if normalized_recipient.is_empty() {
        return RecipientVerificationVerdict::Reject("recipient address is empty".to_string());
    }
    let normalized_sender = sender
        .map(normalize_address)
        .filter(|value| !value.is_empty());
    let cache_key = format!(
        "{}|{}|{}",
        normalized_sender.as_deref().unwrap_or(""),
        normalized_recipient,
        account_id
            .map(|value| value.to_string())
            .unwrap_or_default()
    );

    if let Some(cached) = cached_recipient_verdict(&cache_key) {
        return cached;
    }
    if let Ok(Some(cached)) =
        storage::load_recipient_verification_cache_entry(&config.local_db, &cache_key, unix_now())
            .await
    {
        let verdict = recipient_verdict_from_record(&cached.verdict, cached.detail.clone());
        store_recipient_verdict(
            &cache_key,
            verdict.clone(),
            cached.expires_at_unix.saturating_sub(unix_now()).max(1),
        );
        return verdict;
    }

    let request = RecipientVerificationRequest {
        trace_id: format!("lpe-ct-rcpt-{}", Uuid::new_v4()),
        direction: if account_id.is_some() {
            "smtp-submission".to_string()
        } else {
            "smtp-inbound".to_string()
        },
        sender: normalized_sender,
        recipient: normalized_recipient.clone(),
        helo: helo
            .map(str::trim)
            .map(str::to_string)
            .filter(|value| !value.is_empty()),
        peer: peer
            .map(str::trim)
            .map(str::to_string)
            .filter(|value| !value.is_empty()),
        account_id,
    };

    let signed = match integration_shared_secret().and_then(|secret| {
        SignedIntegrationHeaders::sign(&secret, "POST", RECIPIENT_VERIFICATION_PATH, &request)
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }) {
        Ok(signed) => signed,
        Err(error) => {
            return if config.fail_closed {
                RecipientVerificationVerdict::Defer(format!(
                    "recipient verification bridge is unavailable: {error}"
                ))
            } else {
                RecipientVerificationVerdict::Accept
            };
        }
    };

    let response = match client
        .post(format!(
            "{}{}",
            core_base_url.trim_end_matches('/'),
            RECIPIENT_VERIFICATION_PATH
        ))
        .header(INTEGRATION_KEY_HEADER, &signed.integration_key)
        .header(INTEGRATION_TIMESTAMP_HEADER, &signed.timestamp)
        .header(INTEGRATION_NONCE_HEADER, &signed.nonce)
        .header(INTEGRATION_SIGNATURE_HEADER, &signed.signature)
        .header("x-trace-id", &request.trace_id)
        .json(&request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return if config.fail_closed {
                RecipientVerificationVerdict::Defer(format!(
                    "recipient verification request failed: {error}"
                ))
            } else {
                RecipientVerificationVerdict::Accept
            };
        }
    };

    let mut cache_ttl = config.cache_ttl_seconds;
    let verdict = if response.status().is_success() {
        match response.json::<RecipientVerificationResponse>().await {
            Ok(body) => {
                cache_ttl = body.cache_ttl_seconds.unwrap_or(config.cache_ttl_seconds);
                if body.verified {
                    RecipientVerificationVerdict::Accept
                } else {
                    RecipientVerificationVerdict::Reject(body.detail.unwrap_or_else(|| {
                        "recipient is not authorized for final delivery".to_string()
                    }))
                }
            }
            Err(error) => {
                if config.fail_closed {
                    RecipientVerificationVerdict::Defer(format!(
                        "recipient verification response could not be parsed: {error}"
                    ))
                } else {
                    RecipientVerificationVerdict::Accept
                }
            }
        }
    } else if response.status().is_client_error() {
        let detail = response
            .text()
            .await
            .unwrap_or_else(|_| "recipient verification rejected the recipient".to_string());
        RecipientVerificationVerdict::Reject(detail)
    } else if config.fail_closed {
        let detail = response
            .text()
            .await
            .unwrap_or_else(|_| "recipient verification is temporarily unavailable".to_string());
        RecipientVerificationVerdict::Defer(detail)
    } else {
        RecipientVerificationVerdict::Accept
    };

    store_recipient_verdict(&cache_key, verdict.clone(), cache_ttl.max(1));
    let _ = storage::persist_recipient_verification_cache_entry(
        &config.local_db,
        &storage::RecipientVerificationCacheEntry {
            cache_key,
            sender: request.sender.clone(),
            recipient: request.recipient.clone(),
            account_id: request.account_id.map(|value| value.to_string()),
            verdict: recipient_verdict_label(&verdict).to_string(),
            detail: recipient_verdict_detail(&verdict),
            expires_at_unix: unix_now().saturating_add(cache_ttl.max(1)),
        },
    )
    .await;
    verdict
}

pub(crate) fn evaluate_attachment_policy_with_config<D: Detector>(
    config: &AttachmentPolicyConfig,
    validator: &Validator<D>,
    ingress_context: IngressContext,
    raw_message: &[u8],
) -> Result<AttachmentPolicyVerdict> {
    if config.allow_extensions.is_empty()
        && config.block_extensions.is_empty()
        && config.allow_mime_types.is_empty()
        && config.block_mime_types.is_empty()
        && config.allow_detected_types.is_empty()
        && config.block_detected_types.is_empty()
    {
        return Ok(AttachmentPolicyVerdict::Accept);
    }

    let attachments = collect_mime_attachment_parts(raw_message)?;
    for attachment in attachments {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: lpe_magika::ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        let filename = attachment
            .filename
            .clone()
            .unwrap_or_else(|| "unnamed attachment".to_string());
        let extension = attachment.filename.as_deref().and_then(|value| {
            value
                .rsplit_once('.')
                .and_then(|(_, ext)| normalize_extension_token(ext))
        });
        let declared_mime = attachment
            .declared_mime
            .clone()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let detected_mime = outcome.detected_mime.to_ascii_lowercase();
        let detected_label = outcome.detected_label.to_ascii_lowercase();

        if let Some(rule) = extension
            .as_deref()
            .and_then(|value| match_exact_rule(&config.block_extensions, value))
        {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} matched blocked extension {rule}"
            )));
        }
        if !config.allow_extensions.is_empty()
            && extension
                .as_deref()
                .and_then(|value| match_exact_rule(&config.allow_extensions, value))
                .is_none()
        {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} extension is not present in the allow list"
            )));
        }
        if let Some(rule) = match_exact_rule(&config.block_mime_types, &declared_mime)
            .or_else(|| match_exact_rule(&config.block_mime_types, &detected_mime))
        {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} matched blocked MIME type {rule}"
            )));
        }
        if !config.allow_mime_types.is_empty()
            && match_exact_rule(&config.allow_mime_types, &declared_mime)
                .or_else(|| match_exact_rule(&config.allow_mime_types, &detected_mime))
                .is_none()
        {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} MIME type is not present in the allow list"
            )));
        }
        if let Some(rule) = match_exact_rule(&config.block_detected_types, &detected_label) {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} matched blocked detected type {rule}"
            )));
        }
        if !config.allow_detected_types.is_empty()
            && match_exact_rule(&config.allow_detected_types, &detected_label).is_none()
        {
            return Ok(AttachmentPolicyVerdict::Restrict(format!(
                "attachment {filename} detected type is not present in the allow list"
            )));
        }

        match outcome.policy_decision {
            PolicyDecision::Accept => {}
            PolicyDecision::Restrict | PolicyDecision::Quarantine | PolicyDecision::Reject => {
                return Ok(AttachmentPolicyVerdict::Restrict(format!(
                    "attachment {filename} violated Magika policy: {}",
                    outcome.reason
                )));
            }
        }
    }

    Ok(AttachmentPolicyVerdict::Accept)
}

#[cfg(test)]
pub(crate) fn evaluate_attachment_policy<D: Detector>(
    validator: &Validator<D>,
    ingress_context: IngressContext,
    raw_message: &[u8],
) -> Result<AttachmentPolicyVerdict> {
    let config = attachment_policy_config_from_env();
    evaluate_attachment_policy_with_config(&config, validator, ingress_context, raw_message)
}

#[cfg(test)]
fn address_policy_config_from_env() -> AddressPolicyConfig {
    AddressPolicyConfig {
        allow_senders: parse_csv_env("LPE_CT_POLICY_ALLOW_SENDERS"),
        block_senders: parse_csv_env("LPE_CT_POLICY_BLOCK_SENDERS"),
        allow_recipients: parse_csv_env("LPE_CT_POLICY_ALLOW_RECIPIENTS"),
        block_recipients: parse_csv_env("LPE_CT_POLICY_BLOCK_RECIPIENTS"),
    }
}

#[cfg(test)]
fn attachment_policy_config_from_env() -> AttachmentPolicyConfig {
    AttachmentPolicyConfig {
        allow_extensions: parse_csv_env("LPE_CT_ATTACHMENT_ALLOW_EXTENSIONS"),
        block_extensions: parse_csv_env("LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS"),
        allow_mime_types: parse_csv_env("LPE_CT_ATTACHMENT_ALLOW_MIME_TYPES"),
        block_mime_types: parse_csv_env("LPE_CT_ATTACHMENT_BLOCK_MIME_TYPES"),
        allow_detected_types: parse_csv_env("LPE_CT_ATTACHMENT_ALLOW_DETECTED_TYPES"),
        block_detected_types: parse_csv_env("LPE_CT_ATTACHMENT_BLOCK_DETECTED_TYPES"),
    }
}

#[cfg(test)]
fn parse_csv_env(name: &str) -> Vec<String> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(|entry| entry.trim_start_matches('@').to_ascii_lowercase())
                .filter(|entry| !entry.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn normalize_extension_token(value: &str) -> Option<String> {
    let normalized = value.trim().trim_start_matches('.').to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

fn normalize_address(value: &str) -> String {
    value.trim().trim_matches(['<', '>']).to_ascii_lowercase()
}

fn match_address_rule<'a>(rules: &'a [String], address: &str) -> Option<&'a str> {
    let domain = address
        .rsplit_once('@')
        .map(|(_, domain)| domain)
        .unwrap_or(address);
    rules.iter().find_map(|rule| {
        if rule.contains('@') {
            (rule == address).then_some(rule.as_str())
        } else {
            (rule == domain).then_some(rule.as_str())
        }
    })
}

fn match_exact_rule<'a>(rules: &'a [String], value: &str) -> Option<&'a str> {
    rules
        .iter()
        .find(|rule| rule.as_str() == value)
        .map(|rule| rule.as_str())
}

fn cached_recipient_verdict(key: &str) -> Option<RecipientVerificationVerdict> {
    let cache = RECIPIENT_VERIFICATION_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    let now = unix_now();
    let mut guard = cache.lock().ok()?;
    guard.retain(|_, value| value.expires_at_unix > now);
    guard.get(key).map(|value| value.verdict.clone())
}

fn store_recipient_verdict(key: &str, verdict: RecipientVerificationVerdict, ttl_seconds: u64) {
    let cache = RECIPIENT_VERIFICATION_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Ok(mut guard) = cache.lock() {
        guard.insert(
            key.to_string(),
            CachedRecipientVerdict {
                verdict,
                expires_at_unix: unix_now().saturating_add(ttl_seconds),
            },
        );
    }
}

fn recipient_verdict_label(verdict: &RecipientVerificationVerdict) -> &'static str {
    match verdict {
        RecipientVerificationVerdict::Accept => "accept",
        RecipientVerificationVerdict::Reject(_) => "reject",
        RecipientVerificationVerdict::Defer(_) => "defer",
    }
}

fn recipient_verdict_detail(verdict: &RecipientVerificationVerdict) -> Option<String> {
    match verdict {
        RecipientVerificationVerdict::Accept => None,
        RecipientVerificationVerdict::Reject(detail)
        | RecipientVerificationVerdict::Defer(detail) => Some(detail.clone()),
    }
}

fn recipient_verdict_from_record(
    verdict: &str,
    detail: Option<String>,
) -> RecipientVerificationVerdict {
    match verdict {
        "reject" => RecipientVerificationVerdict::Reject(
            detail.unwrap_or_else(|| "recipient is not authorized for final delivery".to_string()),
        ),
        "defer" => RecipientVerificationVerdict::Defer(
            detail
                .unwrap_or_else(|| "recipient verification is temporarily unavailable".to_string()),
        ),
        _ => RecipientVerificationVerdict::Accept,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_address_policy, evaluate_attachment_policy,
        evaluate_attachment_policy_with_config, verify_recipient_with_core, AddressPolicyVerdict,
        AddressRole, AttachmentPolicyConfig, AttachmentPolicyVerdict, RecipientVerificationConfig,
        RecipientVerificationVerdict, RECIPIENT_VERIFICATION_PATH,
    };
    use crate::env_test_lock;
    use axum::{routing::post, Json, Router};
    use lpe_magika::{DetectionSource, Detector, IngressContext, MagikaDetection, Validator};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn address_policy_supports_exact_and_domain_rules() {
        let _guard = env_test_lock();
        std::env::set_var(
            "LPE_CT_POLICY_BLOCK_SENDERS",
            "bad@example.test,blocked.test",
        );
        std::env::set_var("LPE_CT_POLICY_ALLOW_RECIPIENTS", "allowed.test");

        assert_eq!(
            evaluate_address_policy(AddressRole::Sender, "bad@example.test"),
            AddressPolicyVerdict::Reject(
                "sender bad@example.test matched block list entry bad@example.test".to_string()
            )
        );
        assert!(matches!(
            evaluate_address_policy(AddressRole::Sender, "user@blocked.test"),
            AddressPolicyVerdict::Reject(_)
        ));
        assert_eq!(
            evaluate_address_policy(AddressRole::Recipient, "dest@allowed.test"),
            AddressPolicyVerdict::Allow
        );
        assert!(matches!(
            evaluate_address_policy(AddressRole::Recipient, "dest@other.test"),
            AddressPolicyVerdict::Reject(_)
        ));

        std::env::remove_var("LPE_CT_POLICY_BLOCK_SENDERS");
        std::env::remove_var("LPE_CT_POLICY_ALLOW_RECIPIENTS");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn attachment_policy_checks_extension_and_detected_type() {
        let _guard = env_test_lock();
        std::env::set_var("LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS", "exe");
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pebin".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "Windows executable".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );
        let message = concat!(
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: application/octet-stream; name=\"payload.exe\"\r\n",
            "Content-Disposition: attachment; filename=\"payload.exe\"\r\n",
            "\r\n",
            "MZ\r\n",
            "--b1--\r\n"
        );

        assert!(matches!(
            evaluate_attachment_policy(
                &validator,
                IngressContext::SmtpClientSubmission,
                message.as_bytes()
            )
            .unwrap(),
            AttachmentPolicyVerdict::Restrict(_)
        ));

        std::env::remove_var("LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn attachment_policy_normalizes_leading_dot_extensions() {
        let _guard = env_test_lock();
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pebin".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "Windows executable".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );
        let config = AttachmentPolicyConfig {
            allow_extensions: Vec::new(),
            block_extensions: vec![".exe".to_string()],
            allow_mime_types: Vec::new(),
            block_mime_types: Vec::new(),
            allow_detected_types: Vec::new(),
            block_detected_types: Vec::new(),
        };
        let message = concat!(
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: application/octet-stream; name=\"payload.exe\"\r\n",
            "Content-Disposition: attachment; filename=\"payload.exe\"\r\n",
            "\r\n",
            "MZ\r\n",
            "--b1--\r\n"
        );

        assert!(matches!(
            evaluate_attachment_policy_with_config(
                &config,
                &validator,
                IngressContext::SmtpClientSubmission,
                message.as_bytes()
            )
            .unwrap(),
            AttachmentPolicyVerdict::Restrict(detail)
                if detail.contains("blocked extension .exe")
        ));
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn recipient_verification_uses_internal_api() {
        let _guard = env_test_lock();
        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let captured = Arc::new(Mutex::new(None::<Value>));
        let capture = captured.clone();
        let router = Router::new().route(
            RECIPIENT_VERIFICATION_PATH,
            post(move |Json(payload): Json<Value>| {
                let capture = capture.clone();
                async move {
                    *capture.lock().unwrap() = Some(payload);
                    Json(serde_json::json!({
                        "verified": false,
                        "detail": "unknown local recipient",
                        "cache_ttl_seconds": 60
                    }))
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let config = RecipientVerificationConfig {
            enabled: true,
            fail_closed: true,
            cache_ttl_seconds: 60,
            local_db: crate::storage::LocalDbConfig::default(),
        };

        let verdict = verify_recipient_with_core(
            &reqwest::Client::new(),
            &config,
            &format!("http://{address}"),
            Some("sender@example.test"),
            "missing@example.test",
            Some("mx.example.test"),
            Some("203.0.113.10:25"),
            Some(Uuid::new_v4()),
        )
        .await;

        assert_eq!(
            verdict,
            RecipientVerificationVerdict::Reject("unknown local recipient".to_string())
        );
        let payload = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            payload.get("recipient").and_then(Value::as_str),
            Some("missing@example.test")
        );

        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }
}
