---
type: Rust Function
title: mapi_local_replica_range_is_reserved_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids.rs#L233-L267
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`async fn mapi_local_replica_range_is_reserved_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, range: &MapiLocalReplicaDeletedRange, ) -> Result<bool>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)