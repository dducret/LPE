---
type: Rust Method
title: validate_payload
resource: crates/lpe-domain/src/bridge_auth.rs#L97-L109
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/management_auth/require_integration_request
  - functions/crates/lpe-admin-api/src/integration/require_integration
  - functions/crates/lpe-cli/src/handoff_client_posts_json_and_header
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_validate_for_matching_payload
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_modified_payload
  - functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_stale_timestamps
---

# Signature

`pub fn validate_payload<T: Serialize>( &self, shared_secret: &str, method: &str, path: &str, payload: &T, now: i64, max_skew_seconds: i64, ) -> Result<(), BridgeAuthError>`

# Called by

- [require_integration_request](../../../../../../functions/LPE-CT/src/management_auth/require_integration_request.md)
- [require_integration](../../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [handoff_client_posts_json_and_header](../../../../../../functions/crates/lpe-cli/src/handoff_client_posts_json_and_header.md)
- [signed_headers_validate_for_matching_payload](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_validate_for_matching_payload.md)
- [signed_headers_reject_modified_payload](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_modified_payload.md)
- [signed_headers_reject_stale_timestamps](../../../../../../functions/crates/lpe-domain/src/bridge_auth/signed_headers_reject_stale_timestamps.md)