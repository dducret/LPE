---
type: Rust Method
title: per_user_folder_object_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L688-L696
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
---

# Signature

`pub(in crate::mapi) fn per_user_folder_object_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_read_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response.md)
- [append_write_per_user_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)