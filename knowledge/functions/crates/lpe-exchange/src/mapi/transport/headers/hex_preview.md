---
type: Rust Function
title: hex_preview
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L204-L206
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/hex_lower
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/fast_transfer/summarize_fast_transfer_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/format_inbox_folder_type_getprops_response_context
  - functions/crates/lpe-exchange/src/mapi/session/types/logon_identity_matches_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
---

# Signature

`pub(crate) fn hex_preview(bytes: &[u8], limit: usize) -> String`

# Calls

- [hex_lower](../../../../../../../functions/crates/lpe-domain/src/crypto/hex_lower.md)

# Called by

- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_fast_transfer_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/fast_transfer/summarize_fast_transfer_get_buffer_response.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [format_inbox_folder_type_getprops_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/format_inbox_folder_type_getprops_response_context.md)
- [logon_identity_matches_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/types/logon_identity_matches_store_replica_guid.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)
- [debug_payload_preview_hex](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)