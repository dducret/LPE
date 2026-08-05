---
type: Rust Function
title: preflight_unknown_mapi_navigation_shortcut_delete_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import.rs#L185-L282
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_xid_global_counter
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_reserved_in_tx
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx
---

# Signature

`async fn preflight_unknown_mapi_navigation_shortcut_delete_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, folder_id: u64, source_key: &[u8], ) -> Result<UnknownMapiNavigationShortcutDelete>`

# Calls

- [mapi_store_identity_for_account_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_xid_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_xid_global_counter.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mapi_local_replica_counter_is_reserved_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_reserved_in_tx.md)

# Called by

- [tombstone_unknown_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx.md)