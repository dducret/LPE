---
type: Rust Module
title: bridge_auth
resource: crates/lpe-domain/src/bridge_auth.rs#L1-L288
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-crypto-hmac-sha256-hex-sha256-hex
  - external/serde-serialize
  - external/std-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/super-current-unix-timestamp-bridgeautherror-signedintegrationheaders-default-max-skew-seconds
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [SignedIntegrationHeaders](../../../../classes/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders.md)
- [BridgeAuthError](../../../../classes/crates/lpe-domain/src/bridge_auth/BridgeAuthError.md)
- [fmt](../../../../functions/crates/lpe-domain/src/bridge_auth/BridgeAuthError/std-fmt-display/fmt.md)
- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [sign_with_timestamp_and_nonce](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [validate_payload](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)
- [validate_bytes](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_bytes.md)
- [replay_key](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/replay_key.md)
- [current_unix_timestamp](../../../../functions/crates/lpe-domain/src/bridge_auth/current_unix_timestamp.md)
- [sign_components](../../../../functions/crates/lpe-domain/src/bridge_auth/sign_components.md)
- [SamplePayload](../../../../classes/crates/lpe-domain/src/bridge_auth/SamplePayload.md)
- [signed_headers_validate_for_matching_payload](../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_validate_for_matching_payload.md)
- [signed_headers_reject_modified_payload](../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_modified_payload.md)
- [signed_headers_reject_stale_timestamps](../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_stale_timestamps.md)

# Imports

- `crate::crypto::{hmac_sha256_hex, sha256_hex}`
- `serde::Serialize`
- `std::time::{SystemTime, UNIX_EPOCH}`
- `uuid::Uuid`
- `super::{
        current_unix_timestamp, BridgeAuthError, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS,
    }`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)