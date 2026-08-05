---
type: Rust Method
title: associated_config_message_for_folder_and_source_key_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1393-L1406
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity
---

# Signature

`pub(crate) fn associated_config_message_for_folder_and_source_key_id( &self, folder_id: u64, item_id: u64, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [associated_config_message_for_folder_and_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [current_common_views_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity.md)