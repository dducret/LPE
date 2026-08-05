---
type: Rust Method
title: bookmark
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1088-L1096
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response
---

# Signature

`pub(in crate::mapi) fn bookmark(&self) -> &[u8]`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_row_bookmark_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)
- [rop_free_bookmark_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response.md)