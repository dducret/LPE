---
type: Rust Function
title: mapi_xid
resource: crates/lpe-storage/src/mapi_store_identity.rs#L170-L174
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_move_path
---

# Signature

`pub fn mapi_xid(replica_guid: Uuid, global_counter: u64) -> Vec<u8>`

# Called by

- [commit_mapi_associated_config_update_in_tx](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [mapi_identity_material_for_store_replica](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)
- [exercise_mailbox_move_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_move_path.md)