---
type: Rust Method
title: copy_jmap_email
resource: crates/lpe-exchange/src/tests/mod.rs#L11695-L11731
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn copy_jmap_email<'a>( &'a self, _account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)