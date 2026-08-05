---
type: Rust Function
title: upload_sync_state_stream_with_uploaded_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L539-L578
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
---

# Signature

`pub(crate) fn upload_sync_state_stream_with_uploaded_property( sync_type: u8, current_state: &[u8], property_tag: u32, value: &[u8], ) -> Vec<u8>`

# Calls

- [replguid_globset_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [sync_state_property_value](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value.md)
- [upload_sync_state_stream_from_raw_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)