---
type: Rust Method
title: ensure_public_folder_tree_owner
resource: crates/lpe-exchange/src/tests/mod.rs#L4476-L4484
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_public_folder_child
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_public_folder
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_public_folder
---

# Signature

`fn ensure_public_folder_tree_owner(account_id: Uuid) -> anyhow::Result<()>`

# Called by

- [create_public_folder_child](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_public_folder_child.md)
- [update_public_folder](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_public_folder.md)
- [delete_public_folder](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_public_folder.md)