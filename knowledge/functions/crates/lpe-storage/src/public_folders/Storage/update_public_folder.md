---
type: Rust Method
title: update_public_folder
resource: crates/lpe-storage/src/public_folders.rs#L251-L471
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_tree_admin
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn update_public_folder( &self, input: UpdatePublicFolderInput, audit: AuditEntryInput, ) -> Result<PublicFolder>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_tree_admin](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_tree_admin.md)
- [fetch_public_folder_row](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [record_public_folder_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)