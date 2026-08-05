---
type: Rust Function
title: sync_state_mailboxes_for_excluding_deleted
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L138-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn sync_state_mailboxes_for_excluding_deleted( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], deleted_advertised_special_folders: &HashSet<u64>, ) -> Vec<JmapMailbox>`

# Calls

- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)