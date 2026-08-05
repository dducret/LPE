---
type: Rust Function
title: fetch_mapi_associated_config_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/associated_config_import.rs#L1-L34
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_associated_config_from_row
---

# Signature

`async fn fetch_mapi_associated_config_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, config_id: Uuid, ) -> Result<MapiAssociatedConfigRecord>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mapi_associated_config_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_associated_config_from_row.md)