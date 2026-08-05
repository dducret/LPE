---
type: Rust Method
title: delete_draft_message
resource: crates/lpe-activesync/src/tests.rs#L964-L972
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_draft_message<'a>( &'a self, _account_id: Uuid, message_id: Uuid, _audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)