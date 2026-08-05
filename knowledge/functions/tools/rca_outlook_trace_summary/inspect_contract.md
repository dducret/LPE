---
type: Python Function
title: inspect_contract
resource: tools/rca_outlook_trace_summary.py#L1982-L2023
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/tags_after
  - functions/tools/rca_outlook_trace_summary/problem_tags_after
  - functions/tools/rca_outlook_trace_summary/unknown_named_tags
  - functions/tools/rca_outlook_trace_summary/record_unknown_getprops_tag
  - functions/tools/rca_outlook_trace_summary/record_getprops_problem_tag
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_counts_only_unknown_name_positions
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_normal_view_default_entry_id_not_found_is_expected
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_logon_associated_sharing_provider_not_found_is_expected
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_problem_tag_is_not_counted_as_unknown_success
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_not_found_getprops_uses_associated_config_class
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_not_found_getprops_uses_materialization_request_id
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_non_not_found_getprops_remains_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_context_uses_structured_fields
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_defaulted_tags_are_separated
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_getprops_defaulted_tags_are_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_known_standard_defaulted_tags_are_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_computed_zero_values_are_not_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_non_config_unknown_getprops_defaulted_tags_remain_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_resolved_named_context_suppresses_unknown_getprops_tags
---

# Signature

`def inspect_contract( summary: dict[str, Any], contract: str, fields: dict[str, Any] | None = None ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [tags_after](../../../functions/tools/rca_outlook_trace_summary/tags_after.md)
- [problem_tags_after](../../../functions/tools/rca_outlook_trace_summary/problem_tags_after.md)
- [unknown_named_tags](../../../functions/tools/rca_outlook_trace_summary/unknown_named_tags.md)
- [record_unknown_getprops_tag](../../../functions/tools/rca_outlook_trace_summary/record_unknown_getprops_tag.md)
- [record_getprops_problem_tag](../../../functions/tools/rca_outlook_trace_summary/record_getprops_problem_tag.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [test_unknown_getprops_counts_only_unknown_name_positions](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_counts_only_unknown_name_positions.md)
- [test_calendar_normal_view_default_entry_id_not_found_is_expected](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_normal_view_default_entry_id_not_found_is_expected.md)
- [test_logon_associated_sharing_provider_not_found_is_expected](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_logon_associated_sharing_provider_not_found_is_expected.md)
- [test_unknown_getprops_problem_tag_is_not_counted_as_unknown_success](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_problem_tag_is_not_counted_as_unknown_success.md)
- [test_umolk_not_found_getprops_uses_associated_config_class](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_not_found_getprops_uses_associated_config_class.md)
- [test_umolk_not_found_getprops_uses_materialization_request_id](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_not_found_getprops_uses_materialization_request_id.md)
- [test_umolk_non_not_found_getprops_remains_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_non_not_found_getprops_remains_actionable.md)
- [test_unknown_getprops_context_uses_structured_fields](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_context_uses_structured_fields.md)
- [test_unknown_getprops_defaulted_tags_are_separated](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_defaulted_tags_are_separated.md)
- [test_umolk_getprops_defaulted_tags_are_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_getprops_defaulted_tags_are_actionable.md)
- [test_umolk_known_standard_defaulted_tags_are_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_known_standard_defaulted_tags_are_actionable.md)
- [test_umolk_computed_zero_values_are_not_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_computed_zero_values_are_not_actionable.md)
- [test_non_config_unknown_getprops_defaulted_tags_remain_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_non_config_unknown_getprops_defaulted_tags_remain_actionable.md)
- [test_resolved_named_context_suppresses_unknown_getprops_tags](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_resolved_named_context_suppresses_unknown_getprops_tags.md)