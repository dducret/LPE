---
type: Rust Function
title: log_hierarchy_final_state_debug
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L772-L799
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug
---

# Signature

`fn log_hierarchy_final_state_debug( sync_type: u8, folder_id: u64, summary: &HierarchyTransferDebugSummary, )`

# Called by

- [log_hierarchy_transfer_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug.md)