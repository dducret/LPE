---
type: Rust Function
title: rop_simple_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L428-L432
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_cancel_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/spooler_advisory_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/synchronization_import_deletes_response
---

# Signature

`pub(in crate::mapi) fn rop_simple_success_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [append_set_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)
- [append_set_local_replica_midset_deleted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response.md)
- [append_clone_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response.md)
- [append_stream_region_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response.md)
- [append_set_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_commit_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)
- [append_remove_all_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response.md)
- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [append_set_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [submit_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response.md)
- [abort_submit_cancel_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_cancel_response.md)
- [spooler_advisory_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/spooler_advisory_response.md)
- [append_tell_version_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response.md)
- [append_fast_transfer_destination_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response.md)
- [append_synchronization_open_collector_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)
- [synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/synchronization_import_deletes_response.md)