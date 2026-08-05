---
type: Rust Method
title: bookmark_row_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1098-L1102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
---

# Signature

`pub(in crate::mapi) fn bookmark_row_count(&self) -> Option<i32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_row_bookmark_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)