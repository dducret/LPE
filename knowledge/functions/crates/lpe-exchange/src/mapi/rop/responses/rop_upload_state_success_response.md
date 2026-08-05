---
type: Rust Function
title: rop_upload_state_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L462-L466
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
---

# Signature

`pub(in crate::mapi) fn rop_upload_state_success_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [append_upload_state_stream_begin_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response.md)
- [append_upload_state_stream_continue_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response.md)
- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)