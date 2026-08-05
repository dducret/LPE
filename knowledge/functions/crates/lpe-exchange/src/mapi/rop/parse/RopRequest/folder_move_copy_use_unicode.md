---
type: Rust Method
title: folder_move_copy_use_unicode
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L913-L926
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
---

# Signature

`pub(in crate::mapi) fn folder_move_copy_use_unicode(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_folder_move_copy_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)