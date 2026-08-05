---
type: Rust Function
title: mapi_associated_config_from_row
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L381-L417
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/fetch_mapi_associated_config_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx
---

# Signature

`fn mapi_associated_config_from_row( row: sqlx::postgres::PgRow, ) -> Result<MapiAssociatedConfigRecord>`

# Called by

- [fetch_mapi_associated_config_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/fetch_mapi_associated_config_in_tx.md)
- [upsert_mapi_associated_config_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx.md)