---
type: Rust Function
title: format_inbox_related_release_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1317-L1384
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_release_context_flags_visible_table_setcolumns_without_query_rows
---

# Signature

`pub(super) fn format_inbox_related_release_context( object: Option<&MapiObject>, handle: Option<u32>, state: &PostHierarchyActionState, snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [inbox_release_context_flags_visible_table_setcolumns_without_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_release_context_flags_visible_table_setcolumns_without_query_rows.md)