---
type: Rust Function
title: mapi_navigation_shortcut_from_row
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L354-L379
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/upsert_mapi_navigation_shortcut_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/fetch_mapi_navigation_shortcut_in_tx
---

# Signature

`fn mapi_navigation_shortcut_from_row( row: sqlx::postgres::PgRow, ) -> Result<MapiNavigationShortcutRecord>`

# Called by

- [upsert_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/upsert_mapi_navigation_shortcut_in_tx.md)
- [fetch_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/fetch_mapi_navigation_shortcut_in_tx.md)