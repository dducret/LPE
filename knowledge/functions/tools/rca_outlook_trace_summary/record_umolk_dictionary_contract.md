---
type: Python Function
title: record_umolk_dictionary_contract
resource: tools/rca_outlook_trace_summary.py#L855-L873
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/tags_after
  - functions/tools/rca_outlook_trace_summary/problem_tags_after
  - functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes
---

# Signature

`def record_umolk_dictionary_contract(summary: dict[str, Any], text: str) -> None:`

# Calls

- [tags_after](../../../functions/tools/rca_outlook_trace_summary/tags_after.md)
- [problem_tags_after](../../../functions/tools/rca_outlook_trace_summary/problem_tags_after.md)
- [umolk_dictionary_issue_context](../../../functions/tools/rca_outlook_trace_summary/umolk_dictionary_issue_context.md)

# Called by

- [record_umolk_dictionary_shapes](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes.md)