---
type: Rust Function
title: upload_sync_state_stream_from_sets
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L580-L592
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_hierarchy_change_with_change_number
---

# Signature

`pub(crate) fn upload_sync_state_stream_from_sets( sync_type: u8, normal_change_numbers: &[u64], fai_change_numbers: &[u64], read_change_numbers: &[u64], ) -> Vec<u8>`

# Calls

- [upload_sync_state_stream_from_raw_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties.md)
- [replguid_idset_from_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)

# Called by

- [append_synchronization_get_transfer_state_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [record_sync_upload_content_change](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [record_sync_upload_content_checkpoint](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint.md)
- [record_sync_upload_hierarchy_change_with_change_number](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_hierarchy_change_with_change_number.md)