---
type: Rust Function
title: sync_state_property_value
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L677-L702
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/read_sync_state_u32
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property
---

# Signature

`fn sync_state_property_value(state: &[u8], property_tag: u32) -> Option<Vec<u8>>`

# Calls

- [read_sync_state_u32](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/read_sync_state_u32.md)

# Called by

- [sync_state_stream_with_uploaded_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property.md)
- [upload_sync_state_stream_with_uploaded_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property.md)