---
type: Rust Method
title: category_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1169-L1179
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response
---

# Signature

`pub(in crate::mapi) fn category_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_expand_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_expand_row_response.md)
- [rop_collapse_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_collapse_row_response.md)