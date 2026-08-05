---
type: Rust Function
title: log_hierarchy_get_buffer_payload_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L561-L620
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_semantic_validation
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
---

# Signature

`pub(crate) fn log_hierarchy_get_buffer_payload_summary( sync_type: u8, folder_id: u64, transfer_status: &str, transfer_buffer: &[u8], )`

# Calls

- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [log_hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_semantic_validation.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)