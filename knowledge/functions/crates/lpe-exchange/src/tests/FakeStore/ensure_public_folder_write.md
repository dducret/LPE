---
type: Rust Method
title: ensure_public_folder_write
resource: crates/lpe-exchange/src/tests/mod.rs#L4525-L4534
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/public_folder_rights_for
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_public_folder_item
---

# Signature

`fn ensure_public_folder_write(&self, account_id: Uuid, folder_id: Uuid) -> anyhow::Result<()>`

# Calls

- [public_folder_rights_for](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/public_folder_rights_for.md)

# Called by

- [upsert_public_folder_item](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_public_folder_item.md)