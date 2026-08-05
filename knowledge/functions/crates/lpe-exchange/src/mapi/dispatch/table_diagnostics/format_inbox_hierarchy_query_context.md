---
type: Rust Function
title: format_inbox_hierarchy_query_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L663-L706
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(super) fn format_inbox_hierarchy_query_context( object: Option<&MapiObject>, request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [hierarchy_row_count_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)