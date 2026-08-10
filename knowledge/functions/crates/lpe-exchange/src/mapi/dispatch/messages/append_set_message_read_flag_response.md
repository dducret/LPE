---
type: Rust Function
title: append_set_message_read_flag_response
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L701-L844
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/flags/read_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_flags
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/flags/unread_from_read_flags
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/properties/rop_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response
---

# Signature

`pub(super) async fn append_set_message_read_flag_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [read_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/read_flags_are_valid.md)
- [read_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_flags.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [unread_from_read_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/flags/unread_from_read_flags.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [rop_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_set_message_read_flag_response.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)

# Called by

- [append_message_state_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response.md)