---
type: Python Function
title: record_umolk_dictionary_shapes
resource: tools/rca_outlook_trace_summary.py#L822-L845
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_contract
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_shapes_are_counted_from_context
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_olprefs_versions_are_counted_from_context
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_flags_zero_olprefs_version
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_contract_flags_requested_missing_dictionary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_contract_ignores_dictionary_not_requested
---

# Signature

`def record_umolk_dictionary_shapes(summary: dict[str, Any], text: str) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [umolk_dictionary_issue_context](../../../functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context.md)
- [record_umolk_dictionary_contract](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_contract.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_umolk_dictionary_shapes_are_counted_from_context](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_shapes_are_counted_from_context.md)
- [test_umolk_dictionary_olprefs_versions_are_counted_from_context](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_olprefs_versions_are_counted_from_context.md)
- [test_umolk_dictionary_flags_zero_olprefs_version](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_flags_zero_olprefs_version.md)
- [test_umolk_dictionary_contract_flags_requested_missing_dictionary](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_contract_flags_requested_missing_dictionary.md)
- [test_umolk_dictionary_contract_ignores_dictionary_not_requested](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_umolk_dictionary_contract_ignores_dictionary_not_requested.md)