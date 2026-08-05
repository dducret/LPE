---
type: Rust Function
title: log_hierarchy_microsoft_payload_comparison
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L801-L867
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug
---

# Signature

`fn log_hierarchy_microsoft_payload_comparison( sync_type: u8, sync_flags: u16, sync_extra_flags: u32, folder_id: u64, requested_property_tags: &[u32], summary: &HierarchyTransferDebugSummary, )`

# Calls

- [hierarchy_microsoft_payload_comparison](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison.md)

# Called by

- [log_hierarchy_transfer_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug.md)