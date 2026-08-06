---
type: Rust Function
title: postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards
resource: crates/lpe-exchange/src/tests/mod.rs#L2630-L2732
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_special_folder_aliases
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`async fn postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [reserve_mapi_local_replica_ids](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [upsert_mapi_special_folder_aliases](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases.md)
- [fetch_mapi_special_folder_aliases](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_special_folder_aliases.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)