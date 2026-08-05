---
type: Rust Function
title: imported_hierarchy_parent_mailbox_id
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L834-L861
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(super) fn imported_hierarchy_parent_mailbox_id( hierarchy_values: &[(u32, MapiValue)], collector_folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<Uuid>`

# Calls

- [source_key_for_mailbox_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)