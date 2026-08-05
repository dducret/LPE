---
type: Rust Function
title: remember_created_mapi_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1333-L1354
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`pub(super) async fn remember_created_mapi_identity<S>( store: &S, principal: &AccountPrincipal, object_kind: MapiIdentityObjectKind, canonical_id: Uuid, reserved_global_counter: Option<u64>, source_key: Option<Vec<u8>>, ) -> Result<u64> where S: ExchangeStore,`

# Calls

- [remember_created_mapi_identity_record](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_move_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)