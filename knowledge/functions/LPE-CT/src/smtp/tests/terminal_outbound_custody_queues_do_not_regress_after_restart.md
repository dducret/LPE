---
type: Rust Function
title: terminal_outbound_custody_queues_do_not_regress_after_restart
resource: LPE-CT/src/smtp/tests.rs#L1929-L1970
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/outbound_request
  - functions/LPE-CT/src/smtp/tests/outbound_terminal_test_message
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/runtime_config
---

# Signature

`async fn terminal_outbound_custody_queues_do_not_regress_after_restart()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [outbound_terminal_test_message](../../../../../functions/LPE-CT/src/smtp/tests/outbound_terminal_test_message.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)