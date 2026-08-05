---
type: Rust Method
title: emails
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L747-L752
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded
---

# Signature

`pub(crate) fn emails(&self) -> Vec<JmapEmail>`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [snapshot_message_counts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)
- [query_rows_ignores_incomplete_windowed_content_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows.md)
- [find_row_beginning_origin_falls_back_when_complete_rows_are_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded.md)