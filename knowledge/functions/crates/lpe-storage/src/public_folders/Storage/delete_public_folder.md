---
type: Rust Method
title: delete_public_folder
resource: crates/lpe-storage/src/public_folders.rs#L473-L574
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_tree_admin
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn delete_public_folder( &self, account_id: Uuid, folder_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_tree_admin](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_tree_admin.md)
- [fetch_public_folder_row](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row.md)
- [record_public_folder_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)