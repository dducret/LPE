---
type: Rust Function
title: log_hierarchy_transfer_debug
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L33-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_final_state_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_microsoft_payload_comparison
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(crate) fn log_hierarchy_transfer_debug( sync_type: u8, sync_flags: u16, sync_extra_flags: u32, folder_id: u64, requested_property_tags: &[u32], transfer_buffer: &[u8], )`

# Calls

- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [log_hierarchy_final_state_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_final_state_debug.md)
- [log_hierarchy_microsoft_payload_comparison](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_microsoft_payload_comparison.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)