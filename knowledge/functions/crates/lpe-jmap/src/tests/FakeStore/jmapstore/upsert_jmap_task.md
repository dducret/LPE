---
type: Rust Method
title: upsert_jmap_task
resource: crates/lpe-jmap/src/tests.rs#L2034-L2096
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/default_task_list
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn upsert_jmap_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask>`

# Calls

- [default_task_list](../../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/default_task_list.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)