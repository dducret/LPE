---
type: Rust Method
title: query_no_advance
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1053-L1055
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn query_no_advance(&self) -> bool`

# Called by

- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [rop_query_rows_response_inner](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)