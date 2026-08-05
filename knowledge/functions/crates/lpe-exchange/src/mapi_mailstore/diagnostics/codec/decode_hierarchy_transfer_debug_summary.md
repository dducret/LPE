---
type: Rust Function
title: decode_hierarchy_transfer_debug_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L74-L196
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_marker
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_u64
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finalize_hierarchy_debug_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/final_sync_state_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_includes_folder_id_with_eid_extra_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_custom_sync_root_and_projects_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_omits_content_activity_count_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts
---

# Signature

`pub(crate) fn decode_hierarchy_transfer_debug_summary( bytes: &[u8], ) -> Result<HierarchyTransferDebugSummary, String>`

# Calls

- [read_debug_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32.md)
- [hierarchy_debug_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_marker.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)
- [parse_debug_fast_transfer_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)
- [collect_final_state_debug_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property.md)
- [decode_debug_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z.md)
- [decode_debug_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id.md)
- [decode_debug_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_u64.md)
- [decode_debug_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number.md)
- [decode_debug_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32.md)
- [decode_debug_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool.md)
- [finalize_hierarchy_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finalize_hierarchy_debug_summary.md)

# Called by

- [log_hierarchy_transfer_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_transfer_debug.md)
- [log_hierarchy_get_buffer_payload_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary.md)
- [hierarchy_transfer_close_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary.md)
- [default_folder_hierarchy_membership_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary.md)
- [final_sync_state_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/final_sync_state_debug_summary.md)
- [hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)
- [root_hierarchy_transfer_ipm_subtree_reports_virtual_children](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children.md)
- [hierarchy_transfer_debug_decoder_summarizes_serialized_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream.md)
- [ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules.md)
- [hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape.md)
- [hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters.md)
- [hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id.md)
- [hierarchy_transfer_includes_folder_id_with_eid_extra_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_includes_folder_id_with_eid_extra_flag.md)
- [hierarchy_transfer_omits_custom_sync_root_and_projects_children](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_custom_sync_root_and_projects_children.md)
- [hierarchy_sync_omits_content_activity_count_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_omits_content_activity_count_properties.md)
- [hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts.md)