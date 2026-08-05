---
type: Rust Method
title: seek_row_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1121-L1124
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response
---

# Signature

`pub(in crate::mapi) fn seek_row_count(&self) -> Option<i32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [rop_seek_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response.md)