---
type: Rust Function
title: upsert_mapi_navigation_shortcut_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import.rs#L10-L96
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_navigation_shortcut_change
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_navigation_shortcut_from_row
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`async fn upsert_mapi_navigation_shortcut_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, input: UpsertMapiNavigationShortcutInput, ) -> Result<MapiNavigationShortcutRecord>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_mapi_navigation_shortcut_change](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_navigation_shortcut_change.md)
- [mapi_navigation_shortcut_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_navigation_shortcut_from_row.md)

# Called by

- [commit_mapi_navigation_shortcut_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)