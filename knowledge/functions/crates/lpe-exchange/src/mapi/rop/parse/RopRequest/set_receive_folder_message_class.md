---
type: Rust Method
title: set_receive_folder_message_class
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L668-L674
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn set_receive_folder_message_class(&self) -> Option<&str>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_set_receive_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)