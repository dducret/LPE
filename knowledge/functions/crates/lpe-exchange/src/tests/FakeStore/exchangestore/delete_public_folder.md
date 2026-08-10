---
type: Rust Method
title: delete_public_folder
resource: crates/lpe-exchange/src/tests/mod.rs#L6399-L6452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_public_folder<'a>( &'a self, principal_account_id: Uuid, folder_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [ensure_public_folder_tree_owner](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)