---
type: Rust Function
title: recipient_verification_uses_internal_api
resource: LPE-CT/src/transport_policy.rs#L683-L740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`async fn recipient_verification_uses_internal_api()`

# Calls

- [env_test_lock](../../../../functions/LPE-CT/src/env_test_lock.md)
- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)