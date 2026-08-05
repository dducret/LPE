---
type: Rust Function
title: canonical_event_writer_advances_the_persisted_mapi_version
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1496-L1530
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/updated_event
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn canonical_event_writer_advances_the_persisted_mapi_version() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [updated_event](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/updated_event.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)