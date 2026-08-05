---
type: Rust Function
title: record_sync_upload_hierarchy_change_with_change_number
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1015-L1049
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(super) fn record_sync_upload_hierarchy_change_with_change_number( session: &mut MapiSession, folder_id: u64, object_id: u64, change_number: u64, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [upload_sync_state_stream_from_sets](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)