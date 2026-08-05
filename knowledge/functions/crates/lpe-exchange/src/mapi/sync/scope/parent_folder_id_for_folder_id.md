---
type: Rust Function
title: parent_folder_id_for_folder_id
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L198-L243
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/mailbox_is_hierarchy_descendant
  - functions/crates/lpe-exchange/src/mapi/sync/scope/folder_is_in_hierarchy_sync_scope
---

# Signature

`fn parent_folder_id_for_folder_id(folder_id: u64, mailboxes: &[JmapMailbox]) -> Option<u64>`

# Called by

- [mailbox_is_hierarchy_descendant](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/mailbox_is_hierarchy_descendant.md)
- [folder_is_in_hierarchy_sync_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/folder_is_in_hierarchy_sync_scope.md)