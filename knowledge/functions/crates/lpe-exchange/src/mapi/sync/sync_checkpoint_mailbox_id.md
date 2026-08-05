---
type: Rust Function
title: sync_checkpoint_mailbox_id
resource: crates/lpe-exchange/src/mapi/sync.rs#L126-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
---

# Signature

`pub(in crate::mapi) fn sync_checkpoint_mailbox_id( folder_id: u64, sync_type: u8, mailboxes: &[JmapMailbox], ) -> Option<Uuid>`

# Calls

- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_synchronization_open_collector_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)