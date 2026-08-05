---
type: Rust Function
title: append_open_embedded_message_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L646-L713
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/transient_embedded_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_open_subject
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_embedded_message_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) async fn append_open_embedded_message_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [open_embedded_message_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/open_embedded_message_source.md)
- [transient_embedded_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/transient_embedded_message_id.md)
- [embedded_message_open_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_open_subject.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_embedded_message_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)