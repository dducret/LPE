---
type: Rust Function
title: collect_final_state_debug_property
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L529-L562
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`pub(super) fn collect_final_state_debug_property( property: &FastTransferDebugProperty, summary: &mut HierarchyTransferDebugSummary, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [format_replguid_globset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)
- [replguid_globset_counters](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)