---
type: Rust Function
title: mapi_identity_repair_preserves_rotated_calendar_change_key
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope.rs#L195-L395
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/scoped_identity_event_input
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_identity_repair_preserves_rotated_calendar_change_key() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [scoped_identity_event_input](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/scoped_identity_event_input.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)