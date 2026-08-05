---
type: Rust Function
title: snapshot_message_counts_for_folder
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1416-L1427
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_email_belongs_to_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
---

# Signature

`pub(super) fn snapshot_message_counts_for_folder( snapshot: &MapiMailStoreSnapshot, folder_id: u64, ) -> (u32, u32)`

# Calls

- [emails](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [snapshot_email_belongs_to_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_email_belongs_to_folder.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)