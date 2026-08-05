---
type: Rust Function
title: load_trace_history
resource: LPE-CT/src/reporting.rs#L456-L477
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/load_trace_history_from_db
  - functions/LPE-CT/src/reporting/read_mail_history_events
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
  called_by:
  - functions/LPE-CT/src/http_routes/trace_history
---

# Signature

`pub(crate) async fn load_trace_history( spool_dir: &Path, config: &RuntimeConfig, trace_id: &str, retention_days: u32, ) -> Result<TraceHistoryDetails>`

# Calls

- [load_trace_history_from_db](../../../../functions/LPE-CT/src/reporting/load_trace_history_from_db.md)
- [read_mail_history_events](../../../../functions/LPE-CT/src/reporting/read_mail_history_events.md)
- [load_trace_details](../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)

# Called by

- [trace_history](../../../../functions/LPE-CT/src/http_routes/trace_history.md)