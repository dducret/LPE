---
type: Rust Function
title: finish_hierarchy_debug_folder
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L606-L735
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_known_parent_source_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/missing_hierarchy_core_property_tags
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_position
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_identity_properties_before_display_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`pub(super) fn finish_hierarchy_debug_folder( folder: HierarchyTransferFolderDebug, seen_source_keys: &mut Vec<Vec<u8>>, summary: &mut HierarchyTransferDebugSummary, )`

# Calls

- [hierarchy_debug_known_parent_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_known_parent_source_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [missing_hierarchy_core_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/missing_hierarchy_core_property_tags.md)
- [property_position](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_position.md)
- [hierarchy_identity_properties_before_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_identity_properties_before_display_name.md)
- [counter_from_xid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid.md)
- [format_debug_hex](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)