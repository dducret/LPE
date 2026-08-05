---
type: Rust Function
title: associated_table_row_id
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L384-L389
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows
---

# Signature

`pub(super) fn associated_table_row_id(message: &AssociatedTableRow) -> u64`

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [sort_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows.md)