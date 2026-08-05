---
type: Rust Function
title: sync_emails_for
resource: crates/lpe-exchange/src/mapi/sync.rs#L57-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
---

# Signature

`pub(in crate::mapi) fn sync_emails_for( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Vec<JmapEmail>`

# Calls

- [emails_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)