---
type: Rust Function
title: reserve_mapi_store_global_counter_range_in_tx
resource: crates/lpe-storage/src/mapi_store_identity.rs#L137-L164
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`pub async fn reserve_mapi_store_global_counter_range_in_tx( tx: &mut Transaction<'_, Postgres>, count: u32, ) -> Result<(MapiStoreIdentity, u64)>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)