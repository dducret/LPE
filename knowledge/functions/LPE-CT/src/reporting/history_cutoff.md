---
type: Rust Function
title: history_cutoff
resource: LPE-CT/src/reporting.rs#L839-L841
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history_from_db
  - functions/LPE-CT/src/reporting/load_trace_history_from_db
  - functions/LPE-CT/src/reporting/prune_transport_audit_jsonl
  - functions/LPE-CT/src/reporting/prune_retained_rows_from_db
---

# Signature

`fn history_cutoff(retention_days: u32) -> i64`

# Called by

- [search_mail_history_from_db](../../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [load_trace_history_from_db](../../../../functions/LPE-CT/src/reporting/load_trace_history_from_db.md)
- [prune_transport_audit_jsonl](../../../../functions/LPE-CT/src/reporting/prune_transport_audit_jsonl.md)
- [prune_retained_rows_from_db](../../../../functions/LPE-CT/src/reporting/prune_retained_rows_from_db.md)