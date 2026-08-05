---
type: Rust Function
title: latest_decision
resource: LPE-CT/src/reporting.rs#L1319-L1325
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

`fn latest_decision(event: &MailHistoryEvent) -> Option<String>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [search_mail_history_from_db](../../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [summarize_trace_history](../../../../functions/LPE-CT/src/reporting/summarize_trace_history.md)