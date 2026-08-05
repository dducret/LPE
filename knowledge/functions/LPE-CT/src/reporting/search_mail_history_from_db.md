---
type: Rust Function
title: search_mail_history_from_db
resource: LPE-CT/src/reporting.rs#L522-L752
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/history_cutoff
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/reporting/mail_history_event_from_row
  - functions/LPE-CT/src/reporting/latest_decision
  - functions/LPE-CT/src/reporting/policy_tags_from_event
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history
---

# Signature

`async fn search_mail_history_from_db( config: &RuntimeConfig, query: &HistoryQuery, retention_days: u32, limit: usize, ) -> Result<Option<MailHistoryResponse>>`

# Calls

- [history_cutoff](../../../../functions/LPE-CT/src/reporting/history_cutoff.md)
- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mail_history_event_from_row](../../../../functions/LPE-CT/src/reporting/mail_history_event_from_row.md)
- [latest_decision](../../../../functions/LPE-CT/src/reporting/latest_decision.md)
- [policy_tags_from_event](../../../../functions/LPE-CT/src/reporting/policy_tags_from_event.md)

# Called by

- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)