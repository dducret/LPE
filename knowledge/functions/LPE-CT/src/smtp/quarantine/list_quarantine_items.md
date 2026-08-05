---
type: Rust Function
title: list_quarantine_items
resource: LPE-CT/src/smtp/quarantine.rs#L33-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_db
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool
  called_by:
  - functions/LPE-CT/src/http_routes/quarantine_items
---

# Signature

`pub(crate) async fn list_quarantine_items( spool_dir: &Path, config: &RuntimeConfig, query: QuarantineQuery, ) -> Result<Vec<QuarantineSummary>>`

# Calls

- [list_quarantine_items_from_db](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_db.md)
- [list_quarantine_items_from_spool](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool.md)

# Called by

- [quarantine_items](../../../../../functions/LPE-CT/src/http_routes/quarantine_items.md)