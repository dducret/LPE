---
type: Rust Method
title: update_jmap_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L10853-L10880
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn update_jmap_mailbox<'a>( &'a self, input: JmapMailboxUpdateInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)