---
type: Python Function
title: record_getprops_problem_tag
resource: tools/rca_outlook_trace_summary.py#L2026-L2084
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/problem_detail_for_tag
  - functions/tools/rca_outlook_trace_summary/problem_is_not_found
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_contract
---

# Signature

`def record_getprops_problem_tag( summary: dict[str, Any], tag: str, contract: str, fields: dict[str, Any] | None, ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [problem_detail_for_tag](../../../functions/tools/rca_outlook_trace_summary/problem_detail_for_tag.md)
- [problem_is_not_found](../../../functions/tools/rca_outlook_trace_summary/problem_is_not_found.md)

# Called by

- [inspect_contract](../../../functions/tools/rca_outlook_trace_summary/inspect_contract.md)