---
type: Rust Method
title: upsert_public_folder_permission
resource: crates/lpe-storage/src/public_folders.rs#L846-L929
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_share
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permission
---

# Signature

`pub async fn upsert_public_folder_permission( &self, input: PublicFolderPermissionInput, audit: AuditEntryInput, ) -> Result<PublicFolderPermission>`

# Calls

- [validate_collaboration_rights](../../../../../../functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights.md)
- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_share](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_share.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [record_public_folder_change_with_extra_affected](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [fetch_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permission.md)