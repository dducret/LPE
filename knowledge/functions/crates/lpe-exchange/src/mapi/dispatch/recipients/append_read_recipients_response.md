---
type: Rust Function
title: append_read_recipients_response
resource: crates/lpe-exchange/src/mapi/dispatch/recipients.rs#L91-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_recipients_reserved
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_recipient_dispatch_response
---

# Signature

`pub(super) fn append_read_recipients_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [read_recipients_reserved](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_recipients_reserved.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)

# Called by

- [append_recipient_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_recipient_dispatch_response.md)