---
type: Rust Function
title: mapi_event_commit_persists_subject_and_attachment_with_one_change
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1390-L1445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_event_commit_persists_subject_and_attachment_with_one_change() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)