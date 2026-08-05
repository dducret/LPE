---
type: Rust Function
title: summarize_trace_history
resource: LPE-CT/src/reporting.rs#L857-L886
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/latest_decision
  - functions/LPE-CT/src/reporting/policy_tags_from_event
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history
---

# Signature

`fn summarize_trace_history(events: Vec<MailHistoryEvent>) -> Option<MailHistorySummary>`

# Calls

- [latest_decision](../../../../functions/LPE-CT/src/reporting/latest_decision.md)
- [policy_tags_from_event](../../../../functions/LPE-CT/src/reporting/policy_tags_from_event.md)

# Called by

- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)