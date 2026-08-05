---
type: Rust Function
title: signed_headers_reject_modified_payload
resource: crates/lpe-domain/src/bridge_auth.rs#L227-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload
---

# Signature

`fn signed_headers_reject_modified_payload()`

# Calls

- [sign_with_timestamp_and_nonce](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [validate_payload](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_payload.md)