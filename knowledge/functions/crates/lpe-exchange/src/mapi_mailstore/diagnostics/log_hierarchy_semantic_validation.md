---
type: Rust Function
title: log_hierarchy_semantic_validation
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L713-L770
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary
---

# Signature

`fn log_hierarchy_semantic_validation( sync_type: u8, folder_id: u64, transfer_status: &str, summary: &HierarchyTransferDebugSummary, )`

# Calls

- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)

# Called by

- [log_hierarchy_get_buffer_payload_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary.md)