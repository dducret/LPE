---
type: Rust Function
title: count_property
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L360-L362
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u32_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants
---

# Signature

`fn count_property(value: Option<MapiValue>) -> Option<u32>`

# Calls

- [u32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u32_property.md)

# Called by

- [classify_outlook_bootstrap_row_invariants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants.md)