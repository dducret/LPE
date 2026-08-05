---
type: Rust Function
title: smtp_data_rejects_with_policy_reason_and_trace
resource: LPE-CT/src/smtp/tests.rs#L1296-L1366
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/plaintext_inbound_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`async fn smtp_data_rejects_with_policy_reason_and_trace()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [plaintext_inbound_store](../../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)