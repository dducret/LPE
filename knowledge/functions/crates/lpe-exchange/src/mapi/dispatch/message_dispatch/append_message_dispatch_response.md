---
type: Rust Function
title: append_message_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/message_dispatch.rs#L18-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_message_dispatch_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, created_emails: &mut Vec<JmapEmail>, ) where S: ExchangeStore,`

# Calls

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_create_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_message_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)
- [append_move_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)