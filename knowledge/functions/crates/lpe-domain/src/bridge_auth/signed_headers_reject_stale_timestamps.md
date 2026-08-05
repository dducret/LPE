---
type: Rust Function
title: signed_headers_reject_stale_timestamps
resource: crates/lpe-domain/src/bridge_auth.rs#L258-L287
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload
---

# Signature

`fn signed_headers_reject_stale_timestamps()`

# Calls

- [sign_with_timestamp_and_nonce](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [validate_payload](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)