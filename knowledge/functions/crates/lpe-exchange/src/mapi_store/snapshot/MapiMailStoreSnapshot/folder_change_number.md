---
type: Rust Method
title: folder_change_number
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L772-L774
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
---

# Signature

`pub(crate) fn folder_change_number(&self, folder_id: u64) -> Option<u64>`

# Calls

- [change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/change_number.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)