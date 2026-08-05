---
type: Rust Function
title: append_upload_state_stream_end_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_upload_state.rs#L191-L399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/validate_download_state_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mark_uploaded_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/uploaded_state_has_delta_anchor
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_upload_state_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_marker_bit
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_upload_state_stream_end_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailbox_email: &str, request_id: &str, responses: &mut Vec<u8>, )`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [replguid_globset_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_debug_summary.md)
- [validate_download_state_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/validate_download_state_property.md)
- [mark_uploaded_state_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mark_uploaded_state_stream.md)
- [sync_state_stream_with_uploaded_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property.md)
- [uploaded_state_has_delta_anchor](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/uploaded_state_has_delta_anchor.md)
- [rop_upload_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_upload_state_success_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [upload_state_marker_bit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_marker_bit.md)
- [upload_sync_state_stream_with_uploaded_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property.md)
- [replguid_globset_counters](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)