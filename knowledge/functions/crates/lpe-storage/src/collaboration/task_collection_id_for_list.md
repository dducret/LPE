---
type: Rust Function
title: task_collection_id_for_list
resource: crates/lpe-storage/src/collaboration.rs#L1545-L1561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_collections
---

# Signature

`fn task_collection_id_for_list( principal_account_id: Uuid, owner_account_id: Uuid, task_list_id: Uuid, role: Option<&str>, ) -> String`

# Calls

- [collection_id_for_owner](../../../../../functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner.md)

# Called by

- [fetch_accessible_task_collections](../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_collections.md)