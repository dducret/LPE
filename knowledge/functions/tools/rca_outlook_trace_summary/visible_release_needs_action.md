---
type: Python Function
title: visible_release_needs_action
resource: tools/rca_outlook_trace_summary.py#L4004-L4015
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/visible_release_descriptor_contract_issue_is_actionable
  - functions/tools/rca_outlook_trace_summary/visible_release_classification_is_actionable
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def visible_release_needs_action(log: dict[str, Any]) -> bool:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [visible_release_descriptor_contract_issue_is_actionable](../../../functions/tools/rca_outlook_trace_summary/visible_release_descriptor_contract_issue_is_actionable.md)
- [visible_release_classification_is_actionable](../../../functions/tools/rca_outlook_trace_summary/visible_release_classification_is_actionable.md)

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)