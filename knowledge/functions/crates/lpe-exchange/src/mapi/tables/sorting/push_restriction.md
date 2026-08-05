---
type: Rust Function
title: push_restriction
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L431-L519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/push_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature
---

# Signature

`fn push_restriction(hash: &mut u64, restriction: &MapiRestriction)`

# Calls

- [push_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/push_bytes.md)

# Called by

- [table_view_signature](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature.md)