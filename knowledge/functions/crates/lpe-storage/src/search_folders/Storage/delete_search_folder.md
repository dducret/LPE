---
type: Rust Method
title: delete_search_folder
resource: crates/lpe-storage/src/search_folders.rs#L393-L449
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
---

# Signature

`pub async fn delete_search_folder( &self, account_id: Uuid, search_folder_id: Uuid, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [insert_collaboration_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)