---
type: Rust Function
title: fetch_mapi_named_property_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L178-L210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_named_property_parts
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn fetch_mapi_named_property_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, property: &MapiNamedProperty, ) -> Result<Option<MapiNamedPropertyMapping>>`

# Calls

- [mapi_named_property_parts](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_named_property_parts.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)