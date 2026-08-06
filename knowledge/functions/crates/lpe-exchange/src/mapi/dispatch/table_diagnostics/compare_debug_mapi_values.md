---
type: Rust Function
title: compare_debug_mapi_values
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1445-L1456
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
---

# Signature

`fn compare_debug_mapi_values(left: Option<MapiValue>, right: Option<MapiValue>) -> Ordering`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)

# Called by

- [sort_debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)