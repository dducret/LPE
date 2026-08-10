---
type: Rust Function
title: mapi_associated_config_delete_tombstones_identity_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/properties.rs#L5223-L5313
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_associated_config_delete_tombstones_identity_in_postgresql() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [upsert_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [fetch_mapi_sync_changes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)
- [delete_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)