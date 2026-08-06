---
type: Rust Method
title: create_public_folder_child
resource: crates/lpe-exchange/src/tests/mod.rs#L6328-L6358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner
  - functions/crates/lpe-exchange/src/tests/FakeStore/public_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_public_folder_child<'a>( &'a self, input: CreatePublicFolderInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, PublicFolder>`

# Calls

- [ensure_public_folder_tree_owner](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner.md)
- [public_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/public_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)