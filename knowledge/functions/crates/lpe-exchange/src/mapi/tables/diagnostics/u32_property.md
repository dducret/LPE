---
type: Rust Function
title: u32_property
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L353-L359
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/count_property
---

# Signature

`fn u32_property(value: Option<MapiValue>) -> Option<u32>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [classify_outlook_bootstrap_row_invariants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants.md)
- [count_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/count_property.md)