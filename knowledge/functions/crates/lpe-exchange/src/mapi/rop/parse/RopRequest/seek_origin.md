---
type: Rust Method
title: seek_origin
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1117-L1119
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response
---

# Signature

`pub(in crate::mapi) fn seek_origin(&self) -> Option<u8>`

# Called by

- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [rop_seek_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response.md)