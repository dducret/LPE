---
type: Rust Function
title: outbound_terminal_test_message
resource: LPE-CT/src/smtp/tests.rs#L3494-L3532
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
---

# Signature

`fn outbound_terminal_test_message( id: &str, status: &str, remote_message_ref: Option<&str>, ) -> QueuedMessage`

# Called by

- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)