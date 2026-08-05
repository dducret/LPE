---
type: Rust Function
title: microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1793-L1973
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids( ) -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [reserve_imported_event_range](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)