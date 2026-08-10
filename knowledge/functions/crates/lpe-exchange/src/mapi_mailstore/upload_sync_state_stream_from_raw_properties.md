---
type: Rust Function
title: upload_sync_state_stream_from_raw_properties
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L633-L648
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets
---

# Signature

`fn upload_sync_state_stream_from_raw_properties( sync_type: u8, cnset_seen: &[u8], cnset_seen_fai: &[u8], cnset_read: &[u8], ) -> Vec<u8>`

# Calls

- [write_binary_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [upload_sync_state_stream_with_uploaded_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property.md)
- [upload_sync_state_stream_from_sets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)