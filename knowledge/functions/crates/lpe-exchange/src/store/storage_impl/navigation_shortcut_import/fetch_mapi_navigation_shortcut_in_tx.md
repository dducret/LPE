---
type: Rust Function
title: fetch_mapi_navigation_shortcut_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import.rs#L98-L121
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_navigation_shortcut_from_row
---

# Signature

`async fn fetch_mapi_navigation_shortcut_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, canonical_id: Uuid, ) -> Result<MapiNavigationShortcutRecord>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mapi_navigation_shortcut_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_navigation_shortcut_from_row.md)