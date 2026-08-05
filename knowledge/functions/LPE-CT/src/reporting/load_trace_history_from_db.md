---
type: Rust Function
title: load_trace_history_from_db
resource: LPE-CT/src/reporting.rs#L754-L791
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/reporting/history_cutoff
  - functions/LPE-CT/src/reporting/mail_history_event_from_row
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
  called_by:
  - functions/LPE-CT/src/reporting/load_trace_history
---

# Signature

`async fn load_trace_history_from_db( spool_dir: &Path, config: &RuntimeConfig, trace_id: &str, retention_days: u32, ) -> Result<Option<TraceHistoryDetails>>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [history_cutoff](../../../../functions/LPE-CT/src/reporting/history_cutoff.md)
- [mail_history_event_from_row](../../../../functions/LPE-CT/src/reporting/mail_history_event_from_row.md)
- [load_trace_details](../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)

# Called by

- [load_trace_history](../../../../functions/LPE-CT/src/reporting/load_trace_history.md)