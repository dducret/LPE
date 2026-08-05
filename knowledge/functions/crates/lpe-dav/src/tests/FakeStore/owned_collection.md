---
type: Rust Method
title: owned_collection
resource: crates/lpe-dav/src/tests.rs#L75-L87
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/tests/FakeStore/contact_collection
  - functions/crates/lpe-dav/src/tests/FakeStore/calendar_collection
---

# Signature

`fn owned_collection(kind: &str, display_name: &str) -> CollaborationCollection`

# Called by

- [contact_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/contact_collection.md)
- [calendar_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/calendar_collection.md)