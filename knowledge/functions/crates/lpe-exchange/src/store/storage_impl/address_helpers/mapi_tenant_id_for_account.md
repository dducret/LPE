---
type: Rust Function
title: mapi_tenant_id_for_account
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L103-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_tenant_id_for_account(storage: &Storage, account_id: Uuid) -> Result<Uuid>`

# Calls

- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)