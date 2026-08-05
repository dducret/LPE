---
type: Rust Function
title: contents_table_open_row_count
resource: crates/lpe-exchange/src/mapi/tables/counts.rs#L169-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(in crate::mapi) fn contents_table_open_row_count( folder_id: u64, associated: bool, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> u32`

# Calls

- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)