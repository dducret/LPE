---
type: Rust Module
title: diagnostics
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L1-L1382
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/codec
  - external/pub-crate-use-codec-decode-content-transfer-fai-debug-summary-decode-hierarchy-transfer-debug-summary-final-sync-state-debug-summary-format-marker-tags-replguid-globset-counters-replguid-globset-debug-summary-contenttransferfaiitemdebug
  - external/pub-crate-use-codec-hierarchy-identity-properties-before-display-name-contenttransferfaidebugsummary
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [hierarchy_parent_source_key_role](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_parent_source_key_role.md)
- [log_hierarchy_transfer_debug](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug.md)
- [log_fai_content_sync_debug](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug.md)
- [fai_debug_item_classification](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification.md)
- [fai_debug_state_origin](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin.md)
- [fai_debug_source_repository](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_source_repository.md)
- [debug_container_class_for_fai_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/debug_container_class_for_fai_folder.md)
- [format_fai_fasttransfer_marker_summary](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/format_fai_fasttransfer_marker_summary.md)
- [format_fai_debug_item_order](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/format_fai_debug_item_order.md)
- [log_hierarchy_get_buffer_payload_summary](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary.md)
- [hierarchy_transfer_close_summary](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary.md)
- [default_folder_hierarchy_membership_summary](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary.md)
- [default_folder_hierarchy_membership_specs](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_specs.md)
- [log_hierarchy_semantic_validation](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_semantic_validation.md)
- [log_hierarchy_final_state_debug](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_final_state_debug.md)
- [log_hierarchy_microsoft_payload_comparison](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_microsoft_payload_comparison.md)
- [hierarchy_property_filter_mode](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_property_filter_mode.md)
- [HierarchyMicrosoftPayloadComparison](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyMicrosoftPayloadComparison.md)
- [hierarchy_microsoft_payload_comparison](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison.md)
- [microsoft_folder_change_required_tags](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/microsoft_folder_change_required_tags.md)
- [HierarchySemanticValidation](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchySemanticValidation.md)
- [hierarchy_semantic_validation](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)
- [root_inclusive_idset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/root_inclusive_idset.md)
- [counter_difference](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/counter_difference.md)
- [format_counter_list](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/format_counter_list.md)
- [HierarchyTransferDebugSummary](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyTransferDebugSummary.md)
- [first_folder_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyTransferDebugSummary/first_folder_name.md)
- [last_folder_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyTransferDebugSummary/last_folder_name.md)
- [HierarchyTransferFolderDebug](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyTransferFolderDebug.md)
- [HierarchyTransferRowDebug](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/HierarchyTransferRowDebug.md)

# Imports

- `codec::*`
- `pub(crate) use codec::{
    decode_content_transfer_fai_debug_summary, decode_hierarchy_transfer_debug_summary,
    final_sync_state_debug_summary, format_marker_tags, replguid_globset_counters,
    replguid_globset_debug_summary, ContentTransferFaiItemDebug,
}`
- `pub(crate) use codec::{
    hierarchy_identity_properties_before_display_name, ContentTransferFaiDebugSummary,
}`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)