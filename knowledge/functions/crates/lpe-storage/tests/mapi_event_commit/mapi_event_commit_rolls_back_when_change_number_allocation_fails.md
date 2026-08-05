---
type: Rust Function
title: mapi_event_commit_rolls_back_when_change_number_allocation_fails
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1583-L1629
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
---

# Signature

`async fn mapi_event_commit_rolls_back_when_change_number_allocation_fails() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)