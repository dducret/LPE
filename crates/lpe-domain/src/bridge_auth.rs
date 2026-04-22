use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const INTEGRATION_KEY_HEADER: &str = "x-lpe-integration-key";
pub const INTEGRATION_TIMESTAMP_HEADER: &str = "x-lpe-integration-timestamp";
pub const INTEGRATION_NONCE_HEADER: &str = "x-lpe-integration-nonce";
pub const INTEGRATION_SIGNATURE_HEADER: &str = "x-lpe-integration-signature";
pub const DEFAULT_MAX_SKEW_SECONDS: i64 = 300;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedIntegrationHeaders {
    pub integration_key: String,
    pub timestamp: String,
    pub nonce: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeAuthError {
    InvalidPayload(String),
    MissingHeader(&'static str),
    InvalidTimestamp(String),
    InvalidSignature,
    TimestampOutsideTolerance {
        timestamp: i64,
        now: i64,
        max_skew_seconds: i64,
    },
}

impl std::fmt::Display for BridgeAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPayload(error) => write!(f, "invalid signed payload: {error}"),
            Self::MissingHeader(header) => write!(f, "missing required integration header {header}"),
            Self::InvalidTimestamp(value) => write!(f, "invalid integration timestamp {value}"),
            Self::InvalidSignature => write!(f, "invalid integration signature"),
            Self::TimestampOutsideTolerance {
                timestamp,
                now,
                max_skew_seconds,
            } => write!(
                f,
                "integration timestamp {timestamp} is outside the allowed skew window around {now} ({max_skew_seconds}s)"
            ),
        }
    }
}

impl std::error::Error for BridgeAuthError {}

impl SignedIntegrationHeaders {
    pub fn sign<T: Serialize>(
        shared_secret: &str,
        method: &str,
        path: &str,
        payload: &T,
    ) -> Result<Self, BridgeAuthError> {
        Self::sign_with_timestamp_and_nonce(
            shared_secret,
            method,
            path,
            payload,
            current_unix_timestamp(),
            Uuid::new_v4().to_string(),
        )
    }

    pub fn sign_with_timestamp_and_nonce<T: Serialize>(
        shared_secret: &str,
        method: &str,
        path: &str,
        payload: &T,
        timestamp: i64,
        nonce: impl Into<String>,
    ) -> Result<Self, BridgeAuthError> {
        let payload = serde_json::to_vec(payload)
            .map_err(|error| BridgeAuthError::InvalidPayload(error.to_string()))?;
        let nonce = nonce.into();
        Ok(Self {
            integration_key: shared_secret.to_string(),
            timestamp: timestamp.to_string(),
            nonce: nonce.clone(),
            signature: sign_components(
                shared_secret,
                method,
                path,
                &timestamp.to_string(),
                &nonce,
                &payload,
            ),
        })
    }

    pub fn validate_payload<T: Serialize>(
        &self,
        shared_secret: &str,
        method: &str,
        path: &str,
        payload: &T,
        now: i64,
        max_skew_seconds: i64,
    ) -> Result<(), BridgeAuthError> {
        let payload = serde_json::to_vec(payload)
            .map_err(|error| BridgeAuthError::InvalidPayload(error.to_string()))?;
        self.validate_bytes(shared_secret, method, path, &payload, now, max_skew_seconds)
    }

    pub fn validate_bytes(
        &self,
        shared_secret: &str,
        method: &str,
        path: &str,
        payload: &[u8],
        now: i64,
        max_skew_seconds: i64,
    ) -> Result<(), BridgeAuthError> {
        let timestamp = self
            .timestamp
            .trim()
            .parse::<i64>()
            .map_err(|_| BridgeAuthError::InvalidTimestamp(self.timestamp.clone()))?;
        if (now - timestamp).abs() > max_skew_seconds {
            return Err(BridgeAuthError::TimestampOutsideTolerance {
                timestamp,
                now,
                max_skew_seconds,
            });
        }

        let expected = sign_components(
            shared_secret,
            method,
            path,
            self.timestamp.trim(),
            self.nonce.trim(),
            payload,
        );
        if expected == self.signature.trim() {
            Ok(())
        } else {
            Err(BridgeAuthError::InvalidSignature)
        }
    }

    pub fn replay_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.timestamp.trim(),
            self.nonce.trim(),
            self.signature.trim()
        )
    }
}

pub fn current_unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn sign_components(
    shared_secret: &str,
    method: &str,
    path: &str,
    timestamp: &str,
    nonce: &str,
    payload: &[u8],
) -> String {
    let body_hash = hex_sha256(payload);
    let mut mac = HmacSha256::new_from_slice(shared_secret.as_bytes())
        .expect("shared secret is always a valid HMAC key");
    mac.update(method.trim().to_ascii_uppercase().as_bytes());
    mac.update(b"\n");
    mac.update(path.trim().as_bytes());
    mac.update(b"\n");
    mac.update(timestamp.trim().as_bytes());
    mac.update(b"\n");
    mac.update(nonce.trim().as_bytes());
    mac.update(b"\n");
    mac.update(body_hash.as_bytes());
    hex_bytes(&mac.finalize().into_bytes())
}

fn hex_sha256(payload: &[u8]) -> String {
    hex_bytes(&Sha256::digest(payload))
}

fn hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{current_unix_timestamp, BridgeAuthError, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS};
    use serde::Serialize;

    #[derive(Serialize)]
    struct SamplePayload {
        value: &'static str,
        count: u32,
    }

    #[test]
    fn signed_headers_validate_for_matching_payload() {
        let payload = SamplePayload {
            value: "alpha",
            count: 2,
        };
        let headers = SignedIntegrationHeaders::sign_with_timestamp_and_nonce(
            "0123456789abcdef0123456789abcdef",
            "POST",
            "/internal/test",
            &payload,
            current_unix_timestamp(),
            "nonce-1",
        )
        .unwrap();

        headers
            .validate_payload(
                "0123456789abcdef0123456789abcdef",
                "POST",
                "/internal/test",
                &payload,
                current_unix_timestamp(),
                DEFAULT_MAX_SKEW_SECONDS,
            )
            .unwrap();
    }

    #[test]
    fn signed_headers_reject_modified_payload() {
        let headers = SignedIntegrationHeaders::sign_with_timestamp_and_nonce(
            "0123456789abcdef0123456789abcdef",
            "POST",
            "/internal/test",
            &SamplePayload {
                value: "alpha",
                count: 2,
            },
            1_700_000_000,
            "nonce-2",
        )
        .unwrap();

        let error = headers
            .validate_payload(
                "0123456789abcdef0123456789abcdef",
                "POST",
                "/internal/test",
                &SamplePayload {
                    value: "beta",
                    count: 2,
                },
                1_700_000_000,
                DEFAULT_MAX_SKEW_SECONDS,
            )
            .unwrap_err();
        assert!(matches!(error, BridgeAuthError::InvalidSignature));
    }

    #[test]
    fn signed_headers_reject_stale_timestamps() {
        let payload = SamplePayload {
            value: "alpha",
            count: 2,
        };
        let headers = SignedIntegrationHeaders::sign_with_timestamp_and_nonce(
            "0123456789abcdef0123456789abcdef",
            "POST",
            "/internal/test",
            &payload,
            100,
            "nonce-3",
        )
        .unwrap();

        let error = headers
            .validate_payload(
                "0123456789abcdef0123456789abcdef",
                "POST",
                "/internal/test",
                &payload,
                1_000,
                60,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            BridgeAuthError::TimestampOutsideTolerance { .. }
        ));
    }
}
