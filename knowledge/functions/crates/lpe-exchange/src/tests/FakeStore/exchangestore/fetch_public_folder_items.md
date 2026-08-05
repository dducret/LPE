---
type: Rust Method
title: fetch_public_folder_items
resource: crates/lpe-exchange/src/tests/mod.rs#L6323-L6340
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read
---

# Signature

`fn fetch_public_folder_items<'a>( &'a self, principal_account_id: Uuid, folder_id: Uuid, ) -> StoreFuture<'a, Vec<PublicFolderItem>>`

# Calls

- [ensure_public_folder_read](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read.md)