---
type: Rust Method
title: shared_read_only_task_collection
resource: crates/lpe-dav/src/tests.rs#L168-L180
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_collection
  called_by:
  - functions/crates/lpe-dav/src/tests/propfind_lists_shared_task_collection_with_canonical_name
  - functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_task_collection
  - functions/crates/lpe-dav/src/tests/delete_returns_forbidden_for_read_only_shared_task
---

# Signature

`fn shared_read_only_task_collection() -> CollaborationCollection`

# Calls

- [shared_collection](../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_collection.md)

# Called by

- [propfind_lists_shared_task_collection_with_canonical_name](../../../../../../functions/crates/lpe-dav/src/tests/propfind_lists_shared_task_collection_with_canonical_name.md)
- [put_returns_forbidden_for_read_only_shared_task_collection](../../../../../../functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_task_collection.md)
- [delete_returns_forbidden_for_read_only_shared_task](../../../../../../functions/crates/lpe-dav/src/tests/delete_returns_forbidden_for_read_only_shared_task.md)