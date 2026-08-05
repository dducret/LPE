---
type: Rust Function
title: outlook_bootstrap_query_rows_total_count
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L46-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn outlook_bootstrap_query_rows_total_count( object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Option<u32>`

# Calls

- [hierarchy_row_count_excluding_deleted](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted.md)
- [restricted_associated_folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)