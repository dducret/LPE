---
type: Rust Function
title: mail_history_event_from_row
resource: LPE-CT/src/reporting.rs#L793-L837
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history_from_db
  - functions/LPE-CT/src/reporting/load_trace_history_from_db
---

# Signature

`fn mail_history_event_from_row(row: &sqlx::postgres::PgRow) -> Result<MailHistoryEvent>`

# Calls

- [try_from](../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [search_mail_history_from_db](../../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [load_trace_history_from_db](../../../../functions/LPE-CT/src/reporting/load_trace_history_from_db.md)