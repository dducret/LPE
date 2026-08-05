---
type: Rust Function
title: hierarchy_virtual_folder_ids
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L152-L159
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_runtime_uses_outlook_safe_folder_projection
---

# Signature

`pub(in crate::mapi) fn hierarchy_virtual_folder_ids(sync_root_folder_id: u64) -> Vec<u64>`

# Called by

- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [ipm_hierarchy_runtime_uses_outlook_safe_folder_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/ipm_hierarchy_runtime_uses_outlook_safe_folder_projection.md)