---
type: Rust Function
title: append_message_status_response
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L1230-L1285
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/status_message_id
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_mask
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_flags
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_message_status_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response
---

# Signature

`pub(super) fn append_message_status_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [status_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/status_message_id.md)
- [mapi_item_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [message_status_mask](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_mask.md)
- [message_status_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_status_flags.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [rop_message_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_message_status_response.md)

# Called by

- [append_message_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response.md)