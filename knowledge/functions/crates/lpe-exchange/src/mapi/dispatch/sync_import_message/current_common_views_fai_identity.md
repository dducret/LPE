---
type: Rust Function
title: current_common_views_fai_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message.rs#L58-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`fn current_common_views_fai_identity( snapshot: &MapiMailStoreSnapshot, message_id: u64, ) -> Result<Option<MapiFaiImportedIdentity>>`

# Calls

- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)
- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)
- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)