---
type: Rust Function
title: load_trace_details
resource: LPE-CT/src/smtp/trace_actions.rs#L3-L8
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/find_message
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
  called_by:
  - functions/LPE-CT/src/http_routes/trace_details
  - functions/LPE-CT/src/reporting/load_trace_history
  - functions/LPE-CT/src/reporting/load_trace_history_from_db
  - functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery
  - functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit
  - functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete
  - functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement
---

# Signature

`pub(crate) fn load_trace_details(spool_dir: &Path, trace_id: &str) -> Result<Option<TraceDetails>>`

# Calls

- [find_message](../../../../../functions/LPE-CT/src/smtp/queue_store/find_message.md)
- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)

# Called by

- [trace_details](../../../../../functions/LPE-CT/src/http_routes/trace_details.md)
- [load_trace_history](../../../../../functions/LPE-CT/src/reporting/load_trace_history.md)
- [load_trace_history_from_db](../../../../../functions/LPE-CT/src/reporting/load_trace_history_from_db.md)
- [accepted_inbound_spool_custody_survives_restart_before_core_delivery](../../../../../functions/LPE-CT/src/smtp/tests/accepted_inbound_spool_custody_survives_restart_before_core_delivery.md)
- [retry_trace_clears_stale_execution_state_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/retry_trace_clears_stale_execution_state_and_appends_audit.md)
- [release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit](../../../../../functions/LPE-CT/src/smtp/tests/release_trace_moves_quarantined_inbound_back_to_incoming_and_appends_audit.md)
- [rejected_quarantine_trace_recovers_from_spool_until_operator_delete](../../../../../functions/LPE-CT/src/smtp/tests/rejected_quarantine_trace_recovers_from_spool_until_operator_delete.md)
- [quarantine_release_reject_delete_recovers_across_node_replacement](../../../../../functions/LPE-CT/src/smtp/tests/quarantine_release_reject_delete_recovers_across_node_replacement.md)