---
type: Rust Function
title: finalize_hierarchy_debug_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L564-L590
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counters_include_all
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`pub(super) fn finalize_hierarchy_debug_summary(summary: &mut HierarchyTransferDebugSummary)`

# Calls

- [counters_include_all](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counters_include_all.md)

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)