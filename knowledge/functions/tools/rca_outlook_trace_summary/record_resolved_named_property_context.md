---
type: Python Function
title: record_resolved_named_property_context
resource: tools/rca_outlook_trace_summary.py#L2141-L2155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_resolved_named_context_suppresses_unknown_getprops_tags
---

# Signature

`def record_resolved_named_property_context( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_resolved_named_context_suppresses_unknown_getprops_tags](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_resolved_named_context_suppresses_unknown_getprops_tags.md)