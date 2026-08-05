---
type: Rust Method
title: shared_collection
resource: crates/lpe-dav/src/tests.rs#L111-L130
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_contact_collection
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_writable_calendar_collection
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_calendar_collection
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_task_collection
---

# Signature

`fn shared_collection( id: &str, kind: &str, owner_account_id: &str, owner_email: &str, owner_display_name: &str, display_name: &str, rights: CollaborationRights, ) -> CollaborationCollection`

# Called by

- [shared_read_only_contact_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_contact_collection.md)
- [shared_writable_calendar_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_writable_calendar_collection.md)
- [shared_read_only_calendar_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_calendar_collection.md)
- [shared_read_only_task_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_task_collection.md)