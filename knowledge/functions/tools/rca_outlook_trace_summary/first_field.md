---
type: Python Function
title: first_field
resource: tools/rca_outlook_trace_summary.py#L1962-L1968
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes
  - functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context
  - functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops
  - functions/tools/rca_outlook_trace_summary/inbox_compact_descriptor_getprops_contract_issues
  - functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup
  - functions/tools/rca_outlook_trace_summary/int_text_field
  - functions/tools/rca_outlook_trace_summary/int_auto_text_field
  - functions/tools/rca_outlook_trace_summary/first_hierarchy_row
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/rca_outlook_trace_summary/record_visible_release_request_metrics
  - functions/tools/rca_outlook_trace_summary/record_visible_release_setcolumns_shape
  - functions/tools/rca_outlook_trace_summary/record_post_visible_release_terminal_event
  - functions/tools/rca_outlook_trace_summary/record_default_view_id_collision
  - functions/tools/rca_outlook_trace_summary/record_inbox_default_view_advertisement
  - functions/tools/rca_outlook_trace_summary/record_visible_release_classification
  - functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
  - functions/tools/rca_outlook_trace_summary/default_view_open_is_special_folder_bootstrap
  - functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key
  - functions/tools/rca_outlook_trace_summary/record_post_calendar_query_position_named_property_probe
  - functions/tools/rca_outlook_trace_summary/record_calendar_contract_fingerprint
  - functions/tools/rca_outlook_trace_summary/record_query_rows_terminal_origin_mismatch
  - functions/tools/rca_outlook_trace_summary/record_descriptor_gap
  - functions/tools/rca_outlook_trace_summary/record_default_view_descriptor_identity_columns
  - functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_window
  - functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_contract
  - functions/tools/rca_outlook_trace_summary/field_in_semicolon_text
  - functions/tools/rca_outlook_trace_summary/inspect_contract
  - functions/tools/rca_outlook_trace_summary/record_getprops_problem_tag
  - functions/tools/rca_outlook_trace_summary/record_unknown_getprops_tag
  - functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable
---

# Signature

`def first_field(text: str, key: str) -> str | None:`

# Called by

- [record_umolk_dictionary_shapes](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes.md)
- [umolk_dictionary_issue_context](../../../functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context.md)
- [record_common_view_descriptor_getprops](../../../functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops.md)
- [inbox_compact_descriptor_getprops_contract_issues](../../../functions/tools/rca_outlook_trace_summary/inbox_compact_descriptor_getprops_contract_issues.md)
- [record_post_visible_release_followup](../../../functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup.md)
- [int_text_field](../../../functions/tools/rca_outlook_trace_summary/int_text_field.md)
- [int_auto_text_field](../../../functions/tools/rca_outlook_trace_summary/int_auto_text_field.md)
- [first_hierarchy_row](../../../functions/tools/rca_outlook_trace_summary/first_hierarchy_row.md)
- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [record_visible_release_request_metrics](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_request_metrics.md)
- [record_visible_release_setcolumns_shape](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_setcolumns_shape.md)
- [record_post_visible_release_terminal_event](../../../functions/tools/rca_outlook_trace_summary/record_post_visible_release_terminal_event.md)
- [record_default_view_id_collision](../../../functions/tools/rca_outlook_trace_summary/record_default_view_id_collision.md)
- [record_inbox_default_view_advertisement](../../../functions/tools/rca_outlook_trace_summary/record_inbox_default_view_advertisement.md)
- [record_visible_release_classification](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_classification.md)
- [record_default_view_query_position_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows.md)
- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)
- [default_view_open_is_special_folder_bootstrap](../../../functions/tools/rca_outlook_trace_summary/default_view_open_is_special_folder_bootstrap.md)
- [default_view_folder_open_setprops_key](../../../functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key.md)
- [record_post_calendar_query_position_named_property_probe](../../../functions/tools/rca_outlook_trace_summary/record_post_calendar_query_position_named_property_probe.md)
- [record_calendar_contract_fingerprint](../../../functions/tools/rca_outlook_trace_summary/record_calendar_contract_fingerprint.md)
- [record_query_rows_terminal_origin_mismatch](../../../functions/tools/rca_outlook_trace_summary/record_query_rows_terminal_origin_mismatch.md)
- [record_descriptor_gap](../../../functions/tools/rca_outlook_trace_summary/record_descriptor_gap.md)
- [record_default_view_descriptor_identity_columns](../../../functions/tools/rca_outlook_trace_summary/record_default_view_descriptor_identity_columns.md)
- [record_visible_release_descriptor_window](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_window.md)
- [record_visible_release_descriptor_contract](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_contract.md)
- [field_in_semicolon_text](../../../functions/tools/rca_outlook_trace_summary/field_in_semicolon_text.md)
- [inspect_contract](../../../functions/tools/rca_outlook_trace_summary/inspect_contract.md)
- [record_getprops_problem_tag](../../../functions/tools/rca_outlook_trace_summary/record_getprops_problem_tag.md)
- [record_unknown_getprops_tag](../../../functions/tools/rca_outlook_trace_summary/record_unknown_getprops_tag.md)
- [descriptor_gap_is_actionable](../../../functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable.md)