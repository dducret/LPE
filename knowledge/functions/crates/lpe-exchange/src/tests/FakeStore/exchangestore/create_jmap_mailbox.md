---
type: Rust Method
title: create_jmap_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L10817-L10851
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_jmap_mailbox<'a>( &'a self, input: JmapMailboxCreateInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)