---
type: Rust Function
title: sync_state_stream_with_uploaded_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L494-L540
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset
---

# Signature

`pub(crate) fn sync_state_stream_with_uploaded_property( sync_type: u8, current_state: &[u8], property_tag: u32, value: &[u8], ) -> Vec<u8>`

# Calls

- [sync_state_property_value](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value.md)
- [sync_state_stream_from_raw_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_download_rejects_malformed_client_globset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset.md)