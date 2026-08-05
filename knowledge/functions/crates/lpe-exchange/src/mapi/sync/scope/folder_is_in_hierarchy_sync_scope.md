---
type: Rust Function
title: folder_is_in_hierarchy_sync_scope
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L245-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/parent_folder_id_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn folder_is_in_hierarchy_sync_scope( folder_id: u64, sync_root_folder_id: u64, mailboxes: &[JmapMailbox], ) -> bool`

# Calls

- [parent_folder_id_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/parent_folder_id_for_folder_id.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)