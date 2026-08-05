---
type: Rust Method
title: fetch_accessible_task_collections
resource: crates/lpe-storage/src/collaboration.rs#L474-L497
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists
  - functions/crates/lpe-storage/src/collaboration/task_collection_id_for_list
---

# Signature

`pub async fn fetch_accessible_task_collections( &self, principal_account_id: Uuid, ) -> Result<Vec<CollaborationCollection>>`

# Calls

- [fetch_task_lists](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)
- [task_collection_id_for_list](../../../../../../functions/crates/lpe-storage/src/collaboration/task_collection_id_for_list.md)