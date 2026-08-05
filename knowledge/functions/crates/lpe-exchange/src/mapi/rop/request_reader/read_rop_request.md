---
type: Rust Function
title: read_rop_request
resource: crates/lpe-exchange/src/mapi/rop/request_reader.rs#L9-L11
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_core_request_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_attachment_request_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_folder_mutation_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/golden_open_folder_rop_round_trips_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/golden_set_columns_rop_round_trips_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_get_contents_table_example_round_trips_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/fast_transfer_source_copy_requests_preserve_send_options
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_set_columns_example_round_trips_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_category_examples_parse_expected_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/expand_row_payload_never_decodes_as_message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_message_move_decodes_length_prefixed_gids
  - functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_deletes_consumes_multibinary_source_keys
  - functions/crates/lpe-exchange/src/mapi/rop/tests/supported_rop_uses_enum_classification_without_terminal_stop
  - functions/crates/lpe-exchange/src/mapi/rop/tests/unsupported_rop_is_terminal_without_consuming_later_rop_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/tests/reserved_rop_is_terminal_and_uses_common_unsupported_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/set_local_replica_midset_deleted_parses_long_term_id_ranges
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_decodes_synchronization_import_read_state_changes
---

# Signature

`pub(in crate::mapi) fn read_rop_request(cursor: &mut Cursor<'_>) -> Result<RopRequest>`

# Calls

- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)

# Called by

- [summarize_request_rop_raw_frames](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames.md)
- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [rop_buffer_is_store_independent_release_only](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [microsoft_oxcmsg_core_request_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_core_request_examples_parse_expected_fields.md)
- [microsoft_oxcmsg_attachment_request_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_attachment_request_examples_parse_expected_fields.md)
- [microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row.md)
- [microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser.md)
- [microsoft_oxcfold_folder_mutation_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_folder_mutation_examples_parse_expected_fields.md)
- [microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields.md)
- [microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags.md)
- [microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields.md)
- [golden_open_folder_rop_round_trips_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/golden_open_folder_rop_round_trips_through_typed_parser.md)
- [golden_set_columns_rop_round_trips_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/golden_set_columns_rop_round_trips_through_typed_parser.md)
- [microsoft_oxctabl_get_contents_table_example_round_trips_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_get_contents_table_example_round_trips_through_typed_parser.md)
- [fast_transfer_source_copy_requests_preserve_send_options](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/fast_transfer_source_copy_requests_preserve_send_options.md)
- [microsoft_oxctabl_set_columns_example_round_trips_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_set_columns_example_round_trips_through_typed_parser.md)
- [microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser.md)
- [microsoft_oxctabl_category_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxctabl_category_examples_parse_expected_fields.md)
- [expand_row_payload_never_decodes_as_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/expand_row_payload_never_decodes_as_message_ids.md)
- [outlook_sync_import_message_move_decodes_length_prefixed_gids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_message_move_decodes_length_prefixed_gids.md)
- [outlook_sync_import_deletes_consumes_multibinary_source_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/outlook_sync_import_deletes_consumes_multibinary_source_keys.md)
- [supported_rop_uses_enum_classification_without_terminal_stop](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/supported_rop_uses_enum_classification_without_terminal_stop.md)
- [unsupported_rop_is_terminal_without_consuming_later_rop_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/unsupported_rop_is_terminal_without_consuming_later_rop_bytes.md)
- [reserved_rop_is_terminal_and_uses_common_unsupported_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/reserved_rop_is_terminal_and_uses_common_unsupported_response.md)
- [set_local_replica_midset_deleted_parses_long_term_id_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/set_local_replica_midset_deleted_parses_long_term_id_ranges.md)
- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)
- [access_plan_decodes_synchronization_import_read_state_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_decodes_synchronization_import_read_state_changes.md)