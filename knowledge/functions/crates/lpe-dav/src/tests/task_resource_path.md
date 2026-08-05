---
type: Rust Function
title: task_resource_path
resource: crates/lpe-dav/src/tests.rs#L26-L28
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/tests/get_returns_vtodo_for_existing_task
  - functions/crates/lpe-dav/src/tests/put_upserts_task_from_vtodo
  - functions/crates/lpe-dav/src/tests/delete_removes_task
  - functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_task_collection
  - functions/crates/lpe-dav/src/tests/delete_returns_forbidden_for_read_only_shared_task
---

# Signature

`fn task_resource_path(collection_id: &str, task_id: Uuid) -> String`

# Called by

- [get_returns_vtodo_for_existing_task](../../../../../functions/crates/lpe-dav/src/tests/get_returns_vtodo_for_existing_task.md)
- [put_upserts_task_from_vtodo](../../../../../functions/crates/lpe-dav/src/tests/put_upserts_task_from_vtodo.md)
- [delete_removes_task](../../../../../functions/crates/lpe-dav/src/tests/delete_removes_task.md)
- [put_returns_forbidden_for_read_only_shared_task_collection](../../../../../functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_task_collection.md)
- [delete_returns_forbidden_for_read_only_shared_task](../../../../../functions/crates/lpe-dav/src/tests/delete_returns_forbidden_for_read_only_shared_task.md)