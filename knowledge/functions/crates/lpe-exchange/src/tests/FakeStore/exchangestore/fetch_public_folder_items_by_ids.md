---
type: Rust Method
title: fetch_public_folder_items_by_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L6473-L6494
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read
---

# Signature

`fn fetch_public_folder_items_by_ids<'a>( &'a self, principal_account_id: Uuid, item_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<PublicFolderItem>>`

# Calls

- [ensure_public_folder_read](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read.md)