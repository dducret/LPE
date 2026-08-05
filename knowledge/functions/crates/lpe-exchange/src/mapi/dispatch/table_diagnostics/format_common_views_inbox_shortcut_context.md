---
type: Rust Function
title: format_common_views_inbox_shortcut_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1082-L1138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(super) fn format_common_views_inbox_shortcut_context( object: Option<&MapiObject>, request: &RopRequest, principal: &AccountPrincipal, snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [query_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [query_forward_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)