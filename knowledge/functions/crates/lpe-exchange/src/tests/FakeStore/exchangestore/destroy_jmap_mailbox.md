---
type: Rust Method
title: destroy_jmap_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L10882-L10899
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn destroy_jmap_mailbox<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)