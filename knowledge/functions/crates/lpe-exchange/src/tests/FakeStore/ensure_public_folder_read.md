---
type: Rust Method
title: ensure_public_folder_read
resource: crates/lpe-exchange/src/tests/mod.rs#L4443-L4452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/public_folder_rights_for
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_public_folder_items
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_public_folder_items_by_ids
---

# Signature

`fn ensure_public_folder_read(&self, account_id: Uuid, folder_id: Uuid) -> anyhow::Result<()>`

# Calls

- [public_folder_rights_for](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/public_folder_rights_for.md)

# Called by

- [fetch_public_folder_items](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_public_folder_items.md)
- [fetch_public_folder_items_by_ids](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_public_folder_items_by_ids.md)