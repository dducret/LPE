---
type: Rust Function
title: rop_buffer_with_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L90-L101
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_batch_is_store_independent
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_with_notification_target_requires_identity_scope
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_distinguishes_truncated_release_prefix
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_release_rops_without_responses
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_display_name_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_folder_type_getprops_probe_stays_store_independent
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_entry_id_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/role_backed_special_folder_getprops_probes_load_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/special_folder_getprops_probe_rejects_custom_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes
  - functions/crates/lpe-exchange/src/mapi/rop/tests/split_rop_buffer_preserves_legacy_framing_when_handle_table_is_valid
---

# Signature

`pub(in crate::mapi) fn rop_buffer_with_response( response: Vec<u8>, output_handles: &[u32], ) -> Vec<u8>`

# Called by

- [parse_execute_rop_dispatch_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)
- [finalize_execute_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer.md)
- [logon_response_debug_summary_decodes_private_mailbox_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields.md)
- [release_only_execute_batch_is_store_independent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_batch_is_store_independent.md)
- [release_only_execute_with_notification_target_requires_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_with_notification_target_requires_identity_scope.md)
- [execute_rop_debug_summary_decodes_ids_and_return_codes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes.md)
- [execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)
- [execute_rop_debug_summary_distinguishes_truncated_release_prefix](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_distinguishes_truncated_release_prefix.md)
- [execute_rop_response_summary_uses_full_truncated_request_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids.md)
- [execute_rop_debug_summary_skips_release_rops_without_responses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_release_rops_without_responses.md)
- [inbox_folder_type_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_probe_loads_store_snapshot.md)
- [inbox_display_name_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_display_name_getprops_probe_loads_store_snapshot.md)
- [root_folder_type_getprops_probe_stays_store_independent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_folder_type_getprops_probe_stays_store_independent.md)
- [root_default_folder_entry_id_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_entry_id_getprops_probe_loads_store_snapshot.md)
- [role_backed_special_folder_getprops_probes_load_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/role_backed_special_folder_getprops_probes_load_store_snapshot.md)
- [special_folder_getprops_probe_rejects_custom_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/special_folder_getprops_probe_rejects_custom_properties.md)
- [first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes.md)
- [post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops.md)
- [first_post_hierarchy_probe_summary_identifies_set_properties_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes.md)
- [split_rop_buffer_preserves_legacy_framing_when_handle_table_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/split_rop_buffer_preserves_legacy_framing_when_handle_table_is_valid.md)