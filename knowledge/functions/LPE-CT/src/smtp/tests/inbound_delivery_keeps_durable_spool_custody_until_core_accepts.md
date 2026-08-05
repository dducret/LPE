---
type: Rust Function
title: inbound_delivery_keeps_durable_spool_custody_until_core_accepts
resource: LPE-CT/src/smtp/tests.rs#L953-L993
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_custody_asserting_core
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/session/receive_message
---

# Signature

`async fn inbound_delivery_keeps_durable_spool_custody_until_core_accepts()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_custody_asserting_core](../../../../../functions/LPE-CT/src/smtp/tests/spawn_custody_asserting_core.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [receive_message](../../../../../functions/LPE-CT/src/smtp/session/receive_message.md)