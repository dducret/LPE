---
type: Rust Function
title: hierarchy_row_count_excluding_deleted
resource: crates/lpe-exchange/src/mapi/tables/counts.rs#L3-L41
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_hierarchy_query_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_post_fai_hierarchy_release_without_inbox_contents_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(in crate::mapi) fn hierarchy_row_count_excluding_deleted( folder_id: u64, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, deleted_advertised_special_folders: &HashSet<u64>, depth: bool, ) -> u32`

# Calls

- [is_queryable_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [hierarchy_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)

# Called by

- [outlook_bootstrap_query_rows_total_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count.md)
- [format_inbox_hierarchy_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_hierarchy_query_context.md)
- [format_post_fai_hierarchy_release_without_inbox_contents_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_post_fai_hierarchy_release_without_inbox_contents_context.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)