---
type: Python Function
title: visible_release_classification_is_actionable
resource: tools/rca_outlook_trace_summary.py#L4026-L4030
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  - functions/tools/rca_outlook_trace_summary/visible_release_needs_action
---

# Signature

`def visible_release_classification_is_actionable(name: str) -> bool:`

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)
- [visible_release_needs_action](../../../functions/tools/rca_outlook_trace_summary/visible_release_needs_action.md)