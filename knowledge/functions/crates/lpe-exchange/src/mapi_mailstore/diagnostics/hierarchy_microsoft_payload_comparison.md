---
type: Rust Function
title: hierarchy_microsoft_payload_comparison
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L901-L1014
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/microsoft_folder_change_required_tags
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_property_filter_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_microsoft_payload_comparison
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules
---

# Signature

`pub(crate) fn hierarchy_microsoft_payload_comparison( sync_flags: u16, sync_extra_flags: u32, _sync_root_folder_id: u64, requested_property_tags: &[u32], summary: &HierarchyTransferDebugSummary, ) -> HierarchyMicrosoftPayloadComparison`

# Calls

- [microsoft_folder_change_required_tags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/microsoft_folder_change_required_tags.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [property_tag_requested](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested.md)
- [hierarchy_property_filter_mode](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_property_filter_mode.md)
- [counter_difference](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference.md)

# Called by

- [log_hierarchy_microsoft_payload_comparison](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_microsoft_payload_comparison.md)
- [hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules.md)