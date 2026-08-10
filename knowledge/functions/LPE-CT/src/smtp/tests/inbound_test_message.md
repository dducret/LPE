---
type: Rust Function
title: inbound_test_message
resource: LPE-CT/src/smtp/tests.rs#L3461-L3492
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery
  - functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement
---

# Signature

`fn inbound_test_message(id: &str, status: &str, subject: &str) -> QueuedMessage`

# Called by

- [accepted_inbound_spool_custody_survives_restart_before_core_delivery](../../../../../functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery.md)
- [quarantine_release_reject_delete_recovers_across_node_replacement](../../../../../functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement.md)