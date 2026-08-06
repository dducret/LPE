---
type: Rust Method
title: commit_mapi_associated_config_create
resource: crates/lpe-exchange/src/tests/mod.rs#L10526-L10568
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql
---

# Signature

`fn commit_mapi_associated_config_create<'a>( &'a self, mut input: crate::store::UpsertMapiAssociatedConfigInput, ) -> StoreFuture<'a, crate::store::MapiAssociatedConfigCommit>`

# Calls

- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [upsert_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [mapi_over_http_online_associated_config_create_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql.md)