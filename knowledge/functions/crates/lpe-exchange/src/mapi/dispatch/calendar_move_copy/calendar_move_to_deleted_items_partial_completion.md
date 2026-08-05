---
type: Rust Function
title: calendar_move_to_deleted_items_partial_completion
resource: crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy.rs#L35-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
---

# Signature

`pub(super) async fn calendar_move_to_deleted_items_partial_completion<S>( store: &S, principal: &AccountPrincipal, request: &RopRequest, source_folder_id: u64, target_folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Option<bool> where S: ExchangeStore,`

# Calls

- [move_copy_want_copy](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [move_copy_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)

# Called by

- [append_move_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)