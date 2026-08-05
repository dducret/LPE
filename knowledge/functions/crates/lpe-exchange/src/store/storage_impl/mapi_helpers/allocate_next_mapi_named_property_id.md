---
type: Rust Function
title: allocate_next_mapi_named_property_id
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L247-L278
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id
---

# Signature

`async fn allocate_next_mapi_named_property_id( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, ) -> Result<u16>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [is_reserved_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id.md)