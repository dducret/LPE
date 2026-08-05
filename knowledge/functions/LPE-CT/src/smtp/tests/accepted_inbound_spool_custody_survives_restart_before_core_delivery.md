---
type: Rust Function
title: accepted_inbound_spool_custody_survives_restart_before_core_delivery
resource: LPE-CT/src/smtp/tests.rs#L1177-L1199
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/inbound_test_message
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
---

# Signature

`async fn accepted_inbound_spool_custody_survives_restart_before_core_delivery()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [inbound_test_message](../../../../../functions/LPE-CT/src/smtp/tests/inbound_test_message.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [load_trace_details](../../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)