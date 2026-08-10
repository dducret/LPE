---
type: Rust Function
title: mapi_identity_allocator_rejects_an_exhausted_global_counter
resource: crates/lpe-exchange/src/tests/mod.rs#L2829-L2858
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
---

# Signature

`async fn mapi_identity_allocator_rejects_an_exhausted_global_counter()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)