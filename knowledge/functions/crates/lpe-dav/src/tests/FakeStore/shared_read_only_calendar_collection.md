---
type: Rust Method
title: shared_read_only_calendar_collection
resource: crates/lpe-dav/src/tests.rs#L156-L166
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_collection
  called_by:
  - functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_calendar_collection
---

# Signature

`fn shared_read_only_calendar_collection() -> CollaborationCollection`

# Calls

- [shared_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_collection.md)

# Called by

- [put_returns_forbidden_for_read_only_shared_calendar_collection](../../../../../../functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_calendar_collection.md)