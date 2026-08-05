---
type: Rust Function
title: rop_get_valid_attachment_numbers_response
resource: crates/lpe-exchange/src/mapi/rop/attachments.rs#L48-L59
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response
---

# Signature

`pub(in crate::mapi) fn rop_get_valid_attachment_numbers_response( request: &RopRequest, attach_nums: &[u32], ) -> Vec<u8>`

# Called by

- [append_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [rop_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response.md)