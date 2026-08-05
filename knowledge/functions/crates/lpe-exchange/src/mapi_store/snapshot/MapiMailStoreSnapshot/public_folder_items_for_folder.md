---
type: Rust Method
title: public_folder_items_for_folder
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L923-L931
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(crate) fn public_folder_items_for_folder( &self, folder_id: u64, ) -> Vec<&MapiPublicFolderItem>`

# Called by

- [hard_delete_public_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)