---
type: Rust Function
title: mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects
resource: crates/lpe-exchange/src/tests/mod.rs#L908-L956
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs
---

# Signature

`async fn mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [upsert_mapi_associated_config](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)
- [fetch_mapi_associated_configs](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_associated_configs.md)