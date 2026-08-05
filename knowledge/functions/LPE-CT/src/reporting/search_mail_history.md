---
type: Rust Function
title: search_mail_history
resource: LPE-CT/src/reporting.rs#L431-L454
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/search_mail_history_from_db
  - functions/LPE-CT/src/reporting/read_mail_history_events
  - functions/LPE-CT/src/reporting/group_history
  - functions/LPE-CT/src/reporting/summarize_trace_history
  - functions/LPE-CT/src/reporting/history_matches
  called_by:
  - functions/LPE-CT/src/http_routes/mail_history
---

# Signature

`pub(crate) async fn search_mail_history( spool_dir: &Path, config: &RuntimeConfig, Query(query): Query<HistoryQuery>, retention_days: u32, ) -> Result<MailHistoryResponse>`

# Calls

- [search_mail_history_from_db](../../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [read_mail_history_events](../../../../functions/LPE-CT/src/reporting/read_mail_history_events.md)
- [group_history](../../../../functions/LPE-CT/src/reporting/group_history.md)
- [summarize_trace_history](../../../../functions/LPE-CT/src/reporting/summarize_trace_history.md)
- [history_matches](../../../../functions/LPE-CT/src/reporting/history_matches.md)

# Called by

- [mail_history](../../../../functions/LPE-CT/src/http_routes/mail_history.md)