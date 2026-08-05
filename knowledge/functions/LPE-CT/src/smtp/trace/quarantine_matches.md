---
type: Rust Function
title: quarantine_matches
resource: LPE-CT/src/smtp/trace.rs#L41-L150
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool
---

# Signature

`pub(in crate::smtp) fn quarantine_matches( item: &QuarantineSummary, query: &QuarantineQuery, ) -> bool`

# Called by

- [list_quarantine_items_from_spool](../../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool.md)