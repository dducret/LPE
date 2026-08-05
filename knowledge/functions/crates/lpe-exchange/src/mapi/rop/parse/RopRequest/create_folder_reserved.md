---
type: Rust Method
title: create_folder_reserved
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L810-L812
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
---

# Signature

`pub(in crate::mapi) fn create_folder_reserved(&self) -> u8`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_create_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)