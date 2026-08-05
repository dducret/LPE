---
type: Rust Method
title: bookmark_want_row_moved_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1104-L1115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
---

# Signature

`pub(in crate::mapi) fn bookmark_want_row_moved_count(&self) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_row_bookmark_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)