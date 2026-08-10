---
type: Rust Function
title: signed_integration_requests_reject_replay
resource: LPE-CT/src/main.rs#L1463-L1519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  - functions/LPE-CT/src/management_auth/require_integration_request
---

# Signature

`fn signed_integration_requests_reject_replay()`

# Calls

- [env_test_lock](../../../functions/LPE-CT/src/env_test_lock.md)
- [sign_with_timestamp_and_nonce](../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [require_integration_request](../../../functions/LPE-CT/src/management_auth/require_integration_request.md)