---
type: Rust Function
title: smtp_ingress_marks_outlook_account_test_message
resource: LPE-CT/src/smtp/tests.rs#L915-L950
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_core
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/session/receive_message
---

# Signature

`async fn smtp_ingress_marks_outlook_account_test_message()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_core](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_core.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [receive_message](../../../../../functions/LPE-CT/src/smtp/session/receive_message.md)