---
type: Rust Method
title: delete_public_folder_permission
resource: crates/lpe-storage/src/public_folders.rs#L931-L990
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_share
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn delete_public_folder_permission( &self, account_id: Uuid, folder_id: Uuid, principal_account_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_share](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_share.md)
- [record_public_folder_change_with_extra_affected](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)