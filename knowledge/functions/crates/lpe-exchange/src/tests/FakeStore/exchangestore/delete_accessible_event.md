---
type: Rust Method
title: delete_accessible_event
resource: crates/lpe-exchange/src/tests/mod.rs#L9168-L9179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_accessible_event<'a>( &'a self, _principal_account_id: Uuid, event_id: Uuid, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)