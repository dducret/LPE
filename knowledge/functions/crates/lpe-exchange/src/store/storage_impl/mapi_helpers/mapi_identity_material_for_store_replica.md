---
type: Rust Function
title: mapi_identity_material_for_store_replica
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L151-L167
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx
---

# Signature

`fn mapi_identity_material_for_store_replica( replica_guid: Uuid, global_counter: u64, ) -> (u64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)`

# Calls

- [mapi_xid](../../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [commit_mapi_associated_config_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx.md)
- [commit_mapi_navigation_shortcut_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx.md)
- [tombstone_unknown_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx.md)