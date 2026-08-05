---
type: Python Function
title: issue_buckets
resource: tools/rca_outlook_trace_summary.py#L3741-L3900
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/visible_release_associated_prefix_issue_buckets
  - functions/tools/rca_outlook_trace_summary/visible_release_needs_action
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/visible_release_descriptor_contract_issue_is_actionable
  - functions/tools/rca_outlook_trace_summary/stable_counter_items
  - functions/tools/rca_outlook_trace_summary/problem_getprops_property_type_counts
  - functions/tools/rca_outlook_trace_summary/visible_release_classification_is_actionable
  - functions/tools/rca_outlook_trace_summary/setcolumns_release_response_handle_classification_is_actionable
  - functions/tools/rca_outlook_trace_summary/post_visible_release_followup_is_actionable
  - functions/tools/rca_outlook_trace_summary/common_views_fai_missing_default_named_view
  - functions/tools/rca_outlook_trace_summary/int_auto_text_field
  - functions/tools/rca_outlook_trace_summary/int_text_field
  - functions/tools/rca_outlook_trace_summary/actionable_zero_default_tag_counts
  - functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable
  - functions/tools/rca_outlook_trace_summary/suppress_explained_symptom_issues
  - functions/tools/rca_outlook_trace_summary/suppress_symptom_only_issues
  called_by:
  - functions/tools/rca_outlook_trace_summary/verdict_for_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/rca_outlook_trace_summary/actionable_issue_buckets
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_non_not_found_getprops_remains_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_getprops_defaulted_tags_are_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_known_standard_defaulted_tags_are_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_flags_old_compact_shape
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_missing_common_view_descriptor_getprops_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_visible_descriptor_gap
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_visible_descriptor_gap_for_backed_columns
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_nonactionable_zero_default_tag
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_flags_empty_structured_folder_view_streams
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_stall_symptoms_for_zero_default_noise
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_stall_symptoms_without_concrete_issue
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_problem_getprops_before_stall_symptoms
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_umolk_problem_getprops_before_stall_symptoms
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_umolk_expected_not_found_getprops
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_problem_getprops_bucket_order_is_stable_for_ties
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_suppresses_release_symptoms_for_folder_local_default_view
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_unexplained_release_symptoms
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_suppresses_visible_inbox_missing_gate_when_rows_tracked
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_folder_local_default_view_visibility_missing_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_requires_startup_row
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_context_only_post_visible_release_followup
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_actionable_post_visible_release_followup
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_query_position_without_rows
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_folder_open_without_rows
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_id_collision
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_complete_projection_visible_release_classification
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_unclassified_visible_release
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_actionable_visible_release_classification
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_account_prefs_first_associated_prefix_find
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_prioritizes_associated_prefix_over_release_symptom
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_post_calendar_named_property_probe
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_setcolumns_release_handle_classifications
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_expected_setcolumns_release_handle_classifications
---

# Signature

`def issue_buckets( rr: dict[str, Any], log: dict[str, Any], log_path: Path | None ) -> list[str]:`

# Calls

- [visible_release_associated_prefix_issue_buckets](../../../functions/tools/rca_outlook_trace_summary/visible_release_associated_prefix_issue_buckets.md)
- [visible_release_needs_action](../../../functions/tools/rca_outlook_trace_summary/visible_release_needs_action.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [visible_release_descriptor_contract_issue_is_actionable](../../../functions/tools/rca_outlook_trace_summary/visible_release_descriptor_contract_issue_is_actionable.md)
- [stable_counter_items](../../../functions/tools/rca_outlook_trace_summary/stable_counter_items.md)
- [problem_getprops_property_type_counts](../../../functions/tools/rca_outlook_trace_summary/problem_getprops_property_type_counts.md)
- [visible_release_classification_is_actionable](../../../functions/tools/rca_outlook_trace_summary/visible_release_classification_is_actionable.md)
- [setcolumns_release_response_handle_classification_is_actionable](../../../functions/tools/rca_outlook_trace_summary/setcolumns_release_response_handle_classification_is_actionable.md)
- [post_visible_release_followup_is_actionable](../../../functions/tools/rca_outlook_trace_summary/post_visible_release_followup_is_actionable.md)
- [common_views_fai_missing_default_named_view](../../../functions/tools/rca_outlook_trace_summary/common_views_fai_missing_default_named_view.md)
- [int_auto_text_field](../../../functions/tools/rca_outlook_trace_summary/int_auto_text_field.md)
- [int_text_field](../../../functions/tools/rca_outlook_trace_summary/int_text_field.md)
- [actionable_zero_default_tag_counts](../../../functions/tools/rca_outlook_trace_summary/actionable_zero_default_tag_counts.md)
- [descriptor_gap_is_actionable](../../../functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable.md)
- [suppress_explained_symptom_issues](../../../functions/tools/rca_outlook_trace_summary/suppress_explained_symptom_issues.md)
- [suppress_symptom_only_issues](../../../functions/tools/rca_outlook_trace_summary/suppress_symptom_only_issues.md)

# Called by

- [verdict_for_summary](../../../functions/tools/rca_outlook_trace_summary/verdict_for_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [actionable_issue_buckets](../../../functions/tools/rca_outlook_trace_summary/actionable_issue_buckets.md)
- [test_umolk_non_not_found_getprops_remains_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_non_not_found_getprops_remains_actionable.md)
- [test_umolk_getprops_defaulted_tags_are_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_getprops_defaulted_tags_are_actionable.md)
- [test_umolk_known_standard_defaulted_tags_are_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_known_standard_defaulted_tags_are_actionable.md)
- [test_visible_inbox_descriptor_contract_flags_old_compact_shape](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_flags_old_compact_shape.md)
- [test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape.md)
- [test_missing_common_view_descriptor_getprops_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_missing_common_view_descriptor_getprops_is_actionable.md)
- [test_issue_buckets_reports_visible_descriptor_gap](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_visible_descriptor_gap.md)
- [test_issue_buckets_ignores_visible_descriptor_gap_for_backed_columns](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_visible_descriptor_gap_for_backed_columns.md)
- [test_issue_buckets_ignores_nonactionable_zero_default_tag](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_nonactionable_zero_default_tag.md)
- [test_issue_buckets_flags_empty_structured_folder_view_streams](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_flags_empty_structured_folder_view_streams.md)
- [test_issue_buckets_keeps_stall_symptoms_for_zero_default_noise](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_stall_symptoms_for_zero_default_noise.md)
- [test_issue_buckets_keeps_stall_symptoms_without_concrete_issue](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_stall_symptoms_without_concrete_issue.md)
- [test_issue_buckets_reports_problem_getprops_before_stall_symptoms](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_problem_getprops_before_stall_symptoms.md)
- [test_issue_buckets_reports_umolk_problem_getprops_before_stall_symptoms](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_umolk_problem_getprops_before_stall_symptoms.md)
- [test_issue_buckets_ignores_umolk_expected_not_found_getprops](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_umolk_expected_not_found_getprops.md)
- [test_problem_getprops_bucket_order_is_stable_for_ties](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_problem_getprops_bucket_order_is_stable_for_ties.md)
- [test_issue_buckets_suppresses_release_symptoms_for_folder_local_default_view](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_suppresses_release_symptoms_for_folder_local_default_view.md)
- [test_issue_buckets_keeps_unexplained_release_symptoms](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_keeps_unexplained_release_symptoms.md)
- [test_issue_buckets_suppresses_visible_inbox_missing_gate_when_rows_tracked](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_suppresses_visible_inbox_missing_gate_when_rows_tracked.md)
- [test_folder_local_default_view_visibility_missing_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_folder_local_default_view_visibility_missing_is_actionable.md)
- [test_broad_ipm_configuration_row_count_gap_requires_startup_row](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_broad_ipm_configuration_row_count_gap_requires_startup_row.md)
- [test_issue_buckets_ignores_context_only_post_visible_release_followup](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_context_only_post_visible_release_followup.md)
- [test_issue_buckets_reports_actionable_post_visible_release_followup](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_actionable_post_visible_release_followup.md)
- [test_issue_buckets_reports_default_view_query_position_without_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_query_position_without_rows.md)
- [test_issue_buckets_reports_default_view_folder_open_without_rows](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_folder_open_without_rows.md)
- [test_issue_buckets_reports_default_view_id_collision](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_default_view_id_collision.md)
- [test_issue_buckets_ignores_complete_projection_visible_release_classification](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_complete_projection_visible_release_classification.md)
- [test_issue_buckets_reports_unclassified_visible_release](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_unclassified_visible_release.md)
- [test_issue_buckets_reports_actionable_visible_release_classification](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_actionable_visible_release_classification.md)
- [test_issue_buckets_reports_account_prefs_first_associated_prefix_find](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_account_prefs_first_associated_prefix_find.md)
- [test_issue_buckets_prioritizes_associated_prefix_over_release_symptom](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_prioritizes_associated_prefix_over_release_symptom.md)
- [test_issue_buckets_reports_post_calendar_named_property_probe](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_post_calendar_named_property_probe.md)
- [test_issue_buckets_reports_setcolumns_release_handle_classifications](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_reports_setcolumns_release_handle_classifications.md)
- [test_issue_buckets_ignores_expected_setcolumns_release_handle_classifications](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_ignores_expected_setcolumns_release_handle_classifications.md)