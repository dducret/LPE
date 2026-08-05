---
type: Rust Function
title: smtp_data_defers_with_trace_when_core_delivery_is_unavailable
resource: LPE-CT/src/smtp/tests.rs#L1068-L1133
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/plaintext_inbound_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`async fn smtp_data_defers_with_trace_when_core_delivery_is_unavailable()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [plaintext_inbound_store](../../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)