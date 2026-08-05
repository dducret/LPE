---
type: Python Function
title: visible_release_associated_prefix_issue_buckets
resource: tools/rca_outlook_trace_summary.py#L3995-L4001
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/visible_release_associated_prefix_find_is_actionable
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def visible_release_associated_prefix_issue_buckets(log: dict[str, Any]) -> list[str]:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [visible_release_associated_prefix_find_is_actionable](../../../functions/tools/rca_outlook_trace_summary/visible_release_associated_prefix_find_is_actionable.md)

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)