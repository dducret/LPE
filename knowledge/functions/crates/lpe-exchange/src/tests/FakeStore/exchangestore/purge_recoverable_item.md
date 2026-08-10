---
type: Rust Method
title: purge_recoverable_item
resource: crates/lpe-exchange/src/tests/mod.rs#L11234-L11259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn purge_recoverable_item<'a>( &'a self, _account_id: Uuid, recoverable_item_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)