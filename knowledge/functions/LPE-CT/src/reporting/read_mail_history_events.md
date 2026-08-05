---
type: Rust Function
title: read_mail_history_events
resource: LPE-CT/src/reporting.rs#L479-L485
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/read_mail_history_events_from_jsonl
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history
  - functions/LPE-CT/src/reporting/load_trace_history
---

# Signature

`async fn read_mail_history_events( spool_dir: &Path, _config: &RuntimeConfig, retention_days: u32, ) -> Result<Vec<MailHistoryEvent>>`

# Calls

- [read_mail_history_events_from_jsonl](../../../../functions/LPE-CT/src/reporting/read_mail_history_events_from_jsonl.md)

# Called by

- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)
- [load_trace_history](../../../../functions/LPE-CT/src/reporting/load_trace_history.md)