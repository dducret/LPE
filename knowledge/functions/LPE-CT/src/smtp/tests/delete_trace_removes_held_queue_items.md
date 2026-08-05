---
type: Rust Function
title: delete_trace_removes_held_queue_items
resource: LPE-CT/src/smtp/tests.rs#L2982-L3028
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

`async fn delete_trace_removes_held_queue_items()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)