---
type: Rust Function
title: delete_trace_rejects_sent_history_items
resource: LPE-CT/src/smtp/tests.rs#L3007-L3051
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/queue_store/persist_message
---

# Signature

`async fn delete_trace_rejects_sent_history_items()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)