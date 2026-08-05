---
type: Rust Function
title: history_matches
resource: LPE-CT/src/reporting.rs#L888-L1001
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history
---

# Signature

`fn history_matches(item: &MailHistorySummary, query: &HistoryQuery) -> bool`

# Called by

- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)