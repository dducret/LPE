---
type: Rust Function
title: mailbox_is_hierarchy_descendant
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L161-L175
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/parent_folder_id_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
---

# Signature

`fn mailbox_is_hierarchy_descendant( mailbox: &JmapMailbox, sync_root_folder_id: u64, mailboxes: &[JmapMailbox], ) -> bool`

# Calls

- [parent_folder_id_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/parent_folder_id_for_folder_id.md)

# Called by

- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)