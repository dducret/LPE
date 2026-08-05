---
type: Rust Method
title: find_origin
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1079-L1082
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/find/find_row
  - functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row
---

# Signature

`pub(in crate::mapi) fn find_origin(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/find/find_row.md)
- [find_hierarchy_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/find/find_hierarchy_row.md)