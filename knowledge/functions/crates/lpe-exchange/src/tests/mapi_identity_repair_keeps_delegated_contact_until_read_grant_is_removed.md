---
type: Rust Function
title: mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed
resource: crates/lpe-exchange/src/tests/mod.rs#L3465-L3618
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`async fn mapi_identity_repair_keeps_delegated_contact_until_read_grant_is_removed()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)