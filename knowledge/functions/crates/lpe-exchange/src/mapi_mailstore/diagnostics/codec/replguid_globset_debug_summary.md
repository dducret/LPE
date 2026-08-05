---
type: Rust Function
title: replguid_globset_debug_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1060-L1062
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/replguid_globset_parser_decodes_push_singleton_client_state
---

# Signature

`pub(crate) fn replguid_globset_debug_summary(value: &[u8]) -> String`

# Calls

- [format_replguid_globset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [replguid_globset_parser_decodes_push_singleton_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/replguid_globset_parser_decodes_push_singleton_client_state.md)