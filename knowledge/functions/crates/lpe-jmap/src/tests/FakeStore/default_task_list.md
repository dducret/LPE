---
type: Rust Method
title: default_task_list
resource: crates/lpe-jmap/src/tests.rs#L724-L743
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/task
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/upsert_jmap_task
  - functions/crates/lpe-jmap/src/tests/canonical_import_and_copy_persist_create_payloads_for_writable_families
---

# Signature

`fn default_task_list() -> ClientTaskList`

# Called by

- [task](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/task.md)
- [upsert_jmap_task](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/upsert_jmap_task.md)
- [canonical_import_and_copy_persist_create_payloads_for_writable_families](../../../../../../functions/crates/lpe-jmap/src/tests/canonical_import_and_copy_persist_create_payloads_for_writable_families.md)