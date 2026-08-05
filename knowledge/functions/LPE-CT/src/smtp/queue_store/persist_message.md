---
type: Rust Function
title: persist_message
resource: LPE-CT/src/smtp/queue_store.rs#L3-L13
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/spool_path
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/queue_store/move_message
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery
  - functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart
  - functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete
  - functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement
  - functions/LPE-CT/src/smtp/tests/delete_trace_removes_held_queue_items
  - functions/LPE-CT/src/smtp/tests/delete_trace_rejects_sent_history_items
---

# Signature

`pub(in crate::smtp) async fn persist_message( spool_dir: &Path, queue: &str, message: &QueuedMessage, ) -> Result<()>`

# Calls

- [spool_path](../../../../../functions/LPE-CT/src/smtp/queue_store/spool_path.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [move_message](../../../../../functions/LPE-CT/src/smtp/queue_store/move_message.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [accepted_inbound_spool_custody_survives_restart_before_core_delivery](../../../../../functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery.md)
- [terminal_outbound_custody_queues_do_not_regress_after_restart](../../../../../functions/LPE-CT/src/smtp/tests/terminal_outbound_custody_queues_do_not_regress_after_restart.md)
- [retry_trace_clears_stale_execution_state_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit.md)
- [release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit.md)
- [rejected_quarantine_trace_recovers_from_spool_until_operator_delete](../../../../../functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete.md)
- [quarantine_release_reject_delete_recovers_across_node_replacement](../../../../../functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement.md)
- [delete_trace_removes_held_queue_items](../../../../../functions/LPE-CT/src/smtp/tests/delete_trace_removes_held_queue_items.md)
- [delete_trace_rejects_sent_history_items](../../../../../functions/LPE-CT/src/smtp/tests/delete_trace_rejects_sent_history_items.md)