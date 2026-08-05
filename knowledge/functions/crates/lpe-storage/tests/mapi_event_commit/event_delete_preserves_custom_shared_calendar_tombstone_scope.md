---
type: Rust Function
title: event_delete_preserves_custom_shared_calendar_tombstone_scope
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L2785-L2902
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn event_delete_preserves_custom_shared_calendar_tombstone_scope() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)