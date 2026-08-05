---
type: Rust Function
title: format_inbox_associated_query_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L708-L773
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(super) fn format_inbox_associated_query_context( object: Option<&MapiObject>, request: &RopRequest, mailbox_guid: Uuid, snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [query_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)