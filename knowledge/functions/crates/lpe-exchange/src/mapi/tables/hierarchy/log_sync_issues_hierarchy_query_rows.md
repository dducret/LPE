---
type: Rust Function
title: log_sync_issues_hierarchy_query_rows
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L448-L509
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/state/selected_row_indexes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn log_sync_issues_hierarchy_query_rows( request: &RopRequest, folder_id: u64, columns: &[u32], restriction: Option<&MapiRestriction>, sort_orders: &[MapiSortOrder], position: usize, rows: &[HierarchyRow<'_>], _mailbox_guid: Uuid, )`

# Calls

- [query_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [selected_row_indexes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/selected_row_indexes.md)
- [query_forward_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)