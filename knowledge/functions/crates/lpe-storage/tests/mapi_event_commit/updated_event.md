---
type: Rust Function
title: updated_event
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L323-L346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/mapi_event_commit/commit_input
  - functions/crates/lpe-storage/tests/mapi_event_commit/canonical_event_writer_advances_the_persisted_mapi_version
---

# Signature

`fn updated_event(fixture: &EventFixture, title: &str) -> UpsertClientEventInput`

# Called by

- [commit_input](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [canonical_event_writer_advances_the_persisted_mapi_version](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/canonical_event_writer_advances_the_persisted_mapi_version.md)