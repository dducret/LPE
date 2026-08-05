---
type: Rust Function
title: list_quarantine_items_from_db
resource: LPE-CT/src/smtp/quarantine.rs#L63-L183
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items
---

# Signature

`async fn list_quarantine_items_from_db( config: &RuntimeConfig, query: &QuarantineQuery, ) -> Result<Option<Vec<QuarantineSummary>>>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [list_quarantine_items](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items.md)