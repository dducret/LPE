---
type: Rust Function
title: postgres_mapi_calendar_fixture_drop_cleans_temporary_schema
resource: crates/lpe-exchange/src/tests/mod.rs#L304-L330
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
---

# Signature

`async fn postgres_mapi_calendar_fixture_drop_cleans_temporary_schema() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)