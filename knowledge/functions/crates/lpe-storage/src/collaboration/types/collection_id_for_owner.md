---
type: Rust Function
title: collection_id_for_owner
resource: crates/lpe-storage/src/collaboration/types.rs#L328-L347
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/shared_collection_id
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal
  - functions/crates/lpe-storage/src/collaboration/task_collection_id_for_list
  - functions/crates/lpe-storage/src/collaboration/types/calendar_collection_id_for_event
---

# Signature

`pub(super) fn collection_id_for_owner( kind: CollaborationResourceKind, principal_account_id: Uuid, owner_account_id: Uuid, role: &str, ) -> String`

# Calls

- [shared_collection_id](../../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id.md)

# Called by

- [fetch_accessible_contacts_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal.md)
- [task_collection_id_for_list](../../../../../../functions/crates/lpe-storage/src/collaboration/task_collection_id_for_list.md)
- [calendar_collection_id_for_event](../../../../../../functions/crates/lpe-storage/src/collaboration/types/calendar_collection_id_for_event.md)