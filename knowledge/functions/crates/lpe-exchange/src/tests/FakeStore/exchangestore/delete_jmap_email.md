---
type: Rust Method
title: delete_jmap_email
resource: crates/lpe-exchange/src/tests/mod.rs#L11614-L11626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_jmap_email<'a>( &'a self, _account_id: Uuid, message_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)