---
type: Rust Function
title: delete_returns_forbidden_for_read_only_shared_task
resource: crates/lpe-dav/src/tests.rs#L1473-L1514
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_task_collection
  - functions/crates/lpe-dav/src/tests/task_resource_path
---

# Signature

`async fn delete_returns_forbidden_for_read_only_shared_task()`

# Calls

- [shared_read_only_task_collection](../../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_task_collection.md)
- [task_resource_path](../../../../../functions/crates/lpe-dav/src/tests/task_resource_path.md)