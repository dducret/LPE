---
type: Rust Function
title: inbound_bridge_failure_keeps_deferred_custody_with_audit
resource: LPE-CT/src/smtp/tests.rs#L1136-L1174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/session/receive_message
---

# Signature

`async fn inbound_bridge_failure_keeps_deferred_custody_with_audit()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [receive_message](../../../../../functions/LPE-CT/src/smtp/session/receive_message.md)