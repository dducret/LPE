---
type: Rust Function
title: mapi_associated_config_storage_is_account_scoped
resource: crates/lpe-exchange/src/tests/mod.rs#L774-L842
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs
---

# Signature

`async fn mapi_associated_config_storage_is_account_scoped()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [upsert_mapi_associated_config](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)
- [fetch_mapi_associated_configs](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs.md)