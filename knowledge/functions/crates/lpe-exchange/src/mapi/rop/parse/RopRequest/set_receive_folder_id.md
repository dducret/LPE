---
type: Rust Method
title: set_receive_folder_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L661-L666
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn set_receive_folder_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_set_receive_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)