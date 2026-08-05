---
type: Rust Method
title: sign_with_timestamp_and_nonce
resource: crates/lpe-domain/src/bridge_auth.rs#L71-L95
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/sign_components
  called_by:
  - functions/LPE-CT/src/signed_integration_requests_reject_replay
  - functions/crates/lpe-admin-api/src/integration/integration_requests_require_signed_headers_and_reject_replay
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_validate_for_matching_payload
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_modified_payload
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_stale_timestamps
---

# Signature

`pub fn sign_with_timestamp_and_nonce<T: Serialize>( shared_secret: &str, method: &str, path: &str, payload: &T, timestamp: i64, nonce: impl Into<String>, ) -> Result<Self, BridgeAuthError>`

# Calls

- [sign_components](../../../../../../functions/crates/lpe-domain/src/bridge_auth/sign_components.md)

# Called by

- [signed_integration_requests_reject_replay](../../../../../../functions/LPE-CT/src/signed_integration_requests_reject_replay.md)
- [integration_requests_require_signed_headers_and_reject_replay](../../../../../../functions/crates/lpe-admin-api/src/integration/integration_requests_require_signed_headers_and_reject_replay.md)
- [sign](../../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [signed_headers_validate_for_matching_payload](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_validate_for_matching_payload.md)
- [signed_headers_reject_modified_payload](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_modified_payload.md)
- [signed_headers_reject_stale_timestamps](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_stale_timestamps.md)