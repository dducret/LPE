---
type: Rust Function
title: format_inbox_associated_find_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L830-L897
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(super) fn format_inbox_associated_find_context( object: Option<&MapiObject>, request: &RopRequest, mailbox_guid: Uuid, snapshot: &MapiMailStoreSnapshot, response: &[u8], ) -> Option<String>`

# Calls

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)