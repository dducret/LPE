---
type: Python Function
title: decrement_default_view_folder_open_without_rows
resource: tools/rca_outlook_trace_summary.py#L1630-L1635
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
  - functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key
  - functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap
---

# Signature

`def decrement_default_view_folder_open_without_rows( summary: dict[str, Any], key: str ) -> None:`

# Called by

- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)
- [refine_default_view_folder_open_without_rows_key](../../../functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key.md)
- [move_default_view_folder_open_without_rows_to_special_folder_bootstrap](../../../functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap.md)