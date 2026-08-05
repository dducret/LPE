---
type: Rust Function
title: rop_create_attachment_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L170-L178
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
---

# Signature

`pub(in crate::mapi) fn rop_create_attachment_response( request: &RopRequest, attach_num: u32, ) -> Vec<u8>`

# Called by

- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)