---
type: Rust Function
title: compare_ordering
resource: crates/lpe-exchange/src/mapi/properties.rs#L1308-L1318
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/compare_i64
---

# Signature

`pub(in crate::mapi) fn compare_ordering(ordering: Ordering, relop: u8) -> bool`

# Called by

- [compare_mapi_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [compare_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_i64.md)