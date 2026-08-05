---
type: Rust Function
title: hierarchy_transfer_close_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L622-L653
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
---

# Signature

`pub(crate) fn hierarchy_transfer_close_summary( sync_type: u8, folder_id: u64, transfer_buffer: &[u8], ) -> String`

# Calls

- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)