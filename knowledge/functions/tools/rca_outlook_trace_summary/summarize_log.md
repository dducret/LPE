---
type: Python Function
title: summarize_log
resource: tools/rca_outlook_trace_summary.py#L383-L639
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/tools/rca_outlook_trace_summary/load_json_line
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/record_mapi_request_session
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
  - functions/tools/rca_outlook_trace_summary/record_query_position_wire_fields
  - functions/tools/rca_outlook_trace_summary/record_post_hierarchy_create_save_submit_metrics
  - functions/tools/rca_outlook_trace_summary/record_query_rows_response_frames
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/rca_outlook_trace_summary/record_setcolumns_release_response
  - functions/tools/rca_outlook_trace_summary/record_mixed_release_queryposition_response
  - functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes
  - functions/tools/rca_outlook_trace_summary/inspect_contract
  - functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops
  - functions/tools/rca_outlook_trace_summary/record_resolved_named_property_context
  - functions/tools/rca_outlook_trace_summary/record_visible_inbox_query_rows
  - functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap
  - functions/tools/rca_outlook_trace_summary/record_draft_message_flag_issue
  - functions/tools/rca_outlook_trace_summary/record_query_rows_terminal_origin_mismatch
  - functions/tools/rca_outlook_trace_summary/record_calendar_contract_fingerprint
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/record_common_views_fai_transfer_summary
  - functions/tools/rca_outlook_trace_summary/record_common_views_fai_content_sync_item
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
  - functions/tools/rca_outlook_trace_summary/record_hierarchy_query_window
  - functions/tools/rca_outlook_trace_summary/record_folder_local_default_view_visibility
  - functions/tools/rca_outlook_trace_summary/record_descriptor_gap
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_summarize_log_reports_truncated_calendar_contract_json
---

# Signature

`def summarize_log(log_path: Path | None) -> dict[str, Any]:`

# Calls

- [open](../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [load_json_line](../../../functions/tools/rca_outlook_trace_summary/load_json_line.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_mapi_request_session](../../../functions/tools/rca_outlook_trace_summary/record_mapi_request_session.md)
- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)
- [record_query_position_wire_fields](../../../functions/tools/rca_outlook_trace_summary/record_query_position_wire_fields.md)
- [record_post_hierarchy_create_save_submit_metrics](../../../functions/tools/rca_outlook_trace_summary/record_post_hierarchy_create_save_submit_metrics.md)
- [record_query_rows_response_frames](../../../functions/tools/rca_outlook_trace_summary/record_query_rows_response_frames.md)
- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [record_setcolumns_release_response](../../../functions/tools/rca_outlook_trace_summary/record_setcolumns_release_response.md)
- [record_mixed_release_queryposition_response](../../../functions/tools/rca_outlook_trace_summary/record_mixed_release_queryposition_response.md)
- [record_post_visible_release_followup](../../../functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup.md)
- [record_umolk_dictionary_shapes](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes.md)
- [inspect_contract](../../../functions/tools/rca_outlook_trace_summary/inspect_contract.md)
- [record_common_view_descriptor_getprops](../../../functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops.md)
- [record_resolved_named_property_context](../../../functions/tools/rca_outlook_trace_summary/record_resolved_named_property_context.md)
- [record_visible_inbox_query_rows](../../../functions/tools/rca_outlook_trace_summary/record_visible_inbox_query_rows.md)
- [record_broad_ipm_configuration_row_count_gap](../../../functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap.md)
- [record_draft_message_flag_issue](../../../functions/tools/rca_outlook_trace_summary/record_draft_message_flag_issue.md)
- [record_query_rows_terminal_origin_mismatch](../../../functions/tools/rca_outlook_trace_summary/record_query_rows_terminal_origin_mismatch.md)
- [record_calendar_contract_fingerprint](../../../functions/tools/rca_outlook_trace_summary/record_calendar_contract_fingerprint.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [record_common_views_fai_transfer_summary](../../../functions/tools/rca_outlook_trace_summary/record_common_views_fai_transfer_summary.md)
- [record_common_views_fai_content_sync_item](../../../functions/tools/rca_outlook_trace_summary/record_common_views_fai_content_sync_item.md)
- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)
- [record_hierarchy_query_window](../../../functions/tools/rca_outlook_trace_summary/record_hierarchy_query_window.md)
- [record_folder_local_default_view_visibility](../../../functions/tools/rca_outlook_trace_summary/record_folder_local_default_view_visibility.md)
- [record_descriptor_gap](../../../functions/tools/rca_outlook_trace_summary/record_descriptor_gap.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_summarize_log_reports_truncated_calendar_contract_json](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_summarize_log_reports_truncated_calendar_contract_json.md)