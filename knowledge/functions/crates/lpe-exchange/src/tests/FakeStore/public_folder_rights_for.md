---
type: Rust Method
title: public_folder_rights_for
resource: crates/lpe-exchange/src/tests/mod.rs#L4361-L4384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_write
  - functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_delete
---

# Signature

`fn public_folder_rights_for( &self, account_id: Uuid, folder_id: Uuid, ) -> anyhow::Result<PublicFolderRights>`

# Called by

- [ensure_public_folder_read](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_read.md)
- [ensure_public_folder_write](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_write.md)
- [ensure_public_folder_delete](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/ensure_public_folder_delete.md)