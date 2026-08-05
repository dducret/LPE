---
type: Rust Function
title: policy_tags_from_event
resource: LPE-CT/src/reporting.rs#L1189-L1221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history_from_db
  - functions/LPE-CT/src/reporting/summarize_trace_history
---

# Signature

`fn policy_tags_from_event(item: &MailHistoryEvent) -> Vec<String>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [search_mail_history_from_db](../../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [summarize_trace_history](../../../../functions/LPE-CT/src/reporting/summarize_trace_history.md)