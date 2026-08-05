---
type: Rust Function
title: mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1281-L1387
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`async fn mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)