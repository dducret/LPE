---
type: Rust Function
title: smtp_data_accepts_null_reverse_path_for_dsn_delivery
resource: LPE-CT/src/smtp/tests.rs#L996-L1065
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_core
  - functions/LPE-CT/src/smtp/tests/plaintext_inbound_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`async fn smtp_data_accepts_null_reverse_path_for_dsn_delivery()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_core](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_core.md)
- [plaintext_inbound_store](../../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)