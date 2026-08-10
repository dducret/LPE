---
type: Rust Function
title: append_set_read_flags_response
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L844-L953
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_asynchronous
  - functions/crates/lpe-exchange/src/mapi/tables/flags/read_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_flags
  - functions/crates/lpe-exchange/src/mapi/tables/flags/unread_from_read_flags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response
---

# Signature

`pub(super) async fn append_set_read_flags_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [want_asynchronous](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_asynchronous.md)
- [read_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/read_flags_are_valid.md)
- [read_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_flags.md)
- [unread_from_read_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/unread_from_read_flags.md)
- [message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [rop_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_read_flags_response.md)
- [mapi_item_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)

# Called by

- [append_message_state_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response.md)