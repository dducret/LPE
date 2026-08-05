---
type: Python Function
title: record_common_views_fai_content_sync_item
resource: tools/rca_outlook_trace_summary.py#L787-L810
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/common_views_named_view_name
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
---

# Signature

`def record_common_views_fai_content_sync_item( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [common_views_named_view_name](../../../functions/tools/rca_outlook_trace_summary/common_views_named_view_name.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)