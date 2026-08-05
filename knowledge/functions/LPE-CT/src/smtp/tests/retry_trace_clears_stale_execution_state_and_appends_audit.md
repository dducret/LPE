---
type: Rust Function
title: retry_trace_clears_stale_execution_state_and_appends_audit
resource: LPE-CT/src/smtp/tests.rs#L2698-L2775
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
---

# Signature

`async fn retry_trace_clears_stale_execution_state_and_appends_audit()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [load_trace_details](../../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)