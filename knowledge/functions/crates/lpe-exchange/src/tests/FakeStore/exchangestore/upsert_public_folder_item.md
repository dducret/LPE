---
type: Rust Method
title: upsert_public_folder_item
resource: crates/lpe-exchange/src/tests/mod.rs#L6627-L6680
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_write
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn upsert_public_folder_item<'a>( &'a self, input: UpsertPublicFolderItemInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, PublicFolderItem>`

# Calls

- [ensure_public_folder_write](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_write.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)