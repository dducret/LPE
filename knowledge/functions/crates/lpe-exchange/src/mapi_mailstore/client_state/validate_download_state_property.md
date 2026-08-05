---
type: Rust Function
title: validate_download_state_property
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L332-L349
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
---

# Signature

`pub(crate) fn validate_download_state_property( sync_type: u8, property_tag: u32, value: &[u8], ) -> Result<(), String>`

# Calls

- [decode_replguid_set](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)