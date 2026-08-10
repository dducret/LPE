---
type: Rust Method
title: update_public_folder
resource: crates/lpe-exchange/src/tests/mod.rs#L6366-L6397
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner
---

# Signature

`fn update_public_folder<'a>( &'a self, input: UpdatePublicFolderInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, PublicFolder>`

# Calls

- [ensure_public_folder_tree_owner](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_tree_owner.md)