---
type: Rust Method
title: delete_public_folder_item
resource: crates/lpe-exchange/src/tests/mod.rs#L6687-L6719
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_delete
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_public_folder_item<'a>( &'a self, principal_account_id: Uuid, folder_id: Uuid, item_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [ensure_public_folder_delete](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_delete.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)