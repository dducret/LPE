---
type: Rust Method
title: associated_config_message_for_folder_and_source_key
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1457-L1469
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/associated_config_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
---

# Signature

`pub(crate) fn associated_config_message_for_folder_and_source_key( &self, folder_id: u64, source_key: &[u8], ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [associated_config_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config_source_key.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)