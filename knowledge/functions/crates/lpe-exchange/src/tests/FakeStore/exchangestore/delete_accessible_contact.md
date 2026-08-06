---
type: Rust Method
title: delete_accessible_contact
resource: crates/lpe-exchange/src/tests/mod.rs#L9020-L9031
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_accessible_contact<'a>( &'a self, _principal_account_id: Uuid, contact_id: Uuid, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)