---
type: Rust Function
title: inbound_message_keeps_non_utf8_raw_bytes
resource: LPE-CT/src/smtp/tests.rs#L2424-L2469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_core
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/runtime_config
---

# Signature

`async fn inbound_message_keeps_non_utf8_raw_bytes()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_core](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_core.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)