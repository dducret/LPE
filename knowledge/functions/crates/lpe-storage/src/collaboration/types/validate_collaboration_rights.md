---
type: Rust Function
title: validate_collaboration_rights
resource: crates/lpe-storage/src/collaboration/types.rs#L310-L326
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
---

# Signature

`pub(crate) fn validate_collaboration_rights( may_read: bool, may_write: bool, may_delete: bool, may_share: bool, ) -> Result<()>`

# Called by

- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [set_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant.md)
- [upsert_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)