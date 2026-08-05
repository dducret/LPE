---
type: Rust Method
title: restore_recoverable_item
resource: crates/lpe-exchange/src/tests/mod.rs#L10925-L10972
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn restore_recoverable_item<'a>( &'a self, _account_id: Uuid, recoverable_item_id: Uuid, target_mailbox_id: Option<Uuid>, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)