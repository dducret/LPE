---
type: Rust Method
title: delegate_freebusy_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1456-L1458
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_computed_delegate_freebusy_messages
---

# Signature

`pub(crate) fn delegate_freebusy_messages(&self) -> &[MapiDelegateFreeBusyMessage]`

# Called by

- [delegate_freebusy_getprops_rejects_message_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [snapshot_projects_computed_delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_computed_delegate_freebusy_messages.md)