---
type: Rust Method
title: attach_num
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L177-L184
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
---

# Signature

`pub(in crate::mapi) fn attach_num(&self) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_open_attachment_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)