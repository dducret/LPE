---
type: Rust Method
title: upsert_dav_task
resource: crates/lpe-dav/src/tests.rs#L618-L678
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/tests/FakeStore/task_collection
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn upsert_dav_task<'a>( &'a self, input: UpsertClientTaskInput, ) -> lpe_mail_auth::StoreFuture<'a, DavTask>`

# Calls

- [task_collection](../../../../../../../functions/crates/lpe-dav/src/tests/FakeStore/task_collection.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)