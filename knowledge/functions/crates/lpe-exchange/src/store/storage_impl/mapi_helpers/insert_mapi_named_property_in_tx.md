---
type: Rust Function
title: insert_mapi_named_property_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L212-L245
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_named_property_parts
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn insert_mapi_named_property_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, property_id: u16, property: &MapiNamedProperty, ) -> Result<()>`

# Calls

- [mapi_named_property_parts](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_named_property_parts.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)