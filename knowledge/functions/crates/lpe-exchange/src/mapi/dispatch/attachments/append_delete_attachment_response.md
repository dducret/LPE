---
type: Rust Function
title: append_delete_attachment_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L529-L644
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/attach_num
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) fn append_delete_attachment_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [attach_num](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/attach_num.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [event_handle_is_writable](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable.md)
- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [attachment_for_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)