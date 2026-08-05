---
type: Rust Method
title: upsert_public_folder_item
resource: crates/lpe-storage/src/public_folders.rs#L634-L742
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_write
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/public_folders/types/map_public_folder_item
---

# Signature

`pub async fn upsert_public_folder_item( &self, input: UpsertPublicFolderItemInput, audit: AuditEntryInput, ) -> Result<PublicFolderItem>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_write](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_write.md)
- [record_public_folder_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [map_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_item.md)