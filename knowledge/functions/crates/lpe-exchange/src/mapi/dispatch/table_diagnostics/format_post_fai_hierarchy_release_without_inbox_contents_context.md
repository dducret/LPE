---
type: Rust Function
title: format_post_fai_hierarchy_release_without_inbox_contents_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L775-L824
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/post_fai_hierarchy_release_context_reports_stop_before_inbox_contents
---

# Signature

`pub(super) fn format_post_fai_hierarchy_release_without_inbox_contents_context( object: Option<&MapiObject>, released_handle: Option<u32>, state: &PostHierarchyActionState, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [hierarchy_row_count_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted.md)

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [post_fai_hierarchy_release_context_reports_stop_before_inbox_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/post_fai_hierarchy_release_context_reports_stop_before_inbox_contents.md)