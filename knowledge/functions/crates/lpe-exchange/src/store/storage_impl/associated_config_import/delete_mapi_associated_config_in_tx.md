---
type: Rust Function
title: delete_mapi_associated_config_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/associated_config_import.rs#L235-L303
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_associated_config_change
---

# Signature

`async fn delete_mapi_associated_config_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, canonical_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_mapi_associated_config_change](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_associated_config_change.md)