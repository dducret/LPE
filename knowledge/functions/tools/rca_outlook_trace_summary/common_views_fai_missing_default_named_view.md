---
type: Python Function
title: common_views_fai_missing_default_named_view
resource: tools/rca_outlook_trace_summary.py#L3903-L3909
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def common_views_fai_missing_default_named_view(log: dict[str, Any], name: str) -> bool:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)