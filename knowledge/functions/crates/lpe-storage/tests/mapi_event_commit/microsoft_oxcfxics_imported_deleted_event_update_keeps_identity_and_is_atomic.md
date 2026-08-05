---
type: Rust Function
title: microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1976-L2301
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic( ) -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [reserve_imported_event_range](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)