---
type: Rust Function
title: mark_uploaded_state_stream
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L938-L940
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_marker_bit
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/uploaded_state_empty_streams_create_delta_anchor
---

# Signature

`pub(super) fn mark_uploaded_state_stream(marker_mask: &mut u8, property_tag: u32)`

# Calls

- [upload_state_marker_bit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_marker_bit.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [uploaded_state_empty_streams_create_delta_anchor](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/uploaded_state_empty_streams_create_delta_anchor.md)