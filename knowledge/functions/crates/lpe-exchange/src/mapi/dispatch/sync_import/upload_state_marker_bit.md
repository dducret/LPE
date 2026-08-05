---
type: Rust Function
title: upload_state_marker_bit
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L924-L932
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mark_uploaded_state_stream
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/uploaded_state_delta_anchor_requires_idset_and_cnset_seen
---

# Signature

`pub(super) fn upload_state_marker_bit(tag: u32) -> u8`

# Called by

- [mark_uploaded_state_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mark_uploaded_state_stream.md)
- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [uploaded_state_delta_anchor_requires_idset_and_cnset_seen](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/uploaded_state_delta_anchor_requires_idset_and_cnset_seen.md)