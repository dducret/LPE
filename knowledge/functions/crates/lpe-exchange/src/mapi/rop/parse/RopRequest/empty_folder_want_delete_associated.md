---
type: Rust Method
title: empty_folder_want_delete_associated
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L976-L984
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
---

# Signature

`pub(in crate::mapi) fn empty_folder_want_delete_associated(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_empty_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)