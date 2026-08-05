---
type: Rust Function
title: integration_requests_require_signed_headers_and_reject_replay
resource: crates/lpe-admin-api/src/integration.rs#L697-L737
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  - functions/crates/lpe-admin-api/src/integration/require_integration
---

# Signature

`fn integration_requests_require_signed_headers_and_reject_replay()`

# Calls

- [sign_with_timestamp_and_nonce](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [require_integration](../../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)