---
type: Python Function
title: default_view_folder_open_setprops_key
resource: tools/rca_outlook_trace_summary.py#L1638-L1644
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
  - functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key
  - functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap
---

# Signature

`def default_view_folder_open_setprops_key(base_key: str, setprops_context: str) -> str:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)
- [refine_default_view_folder_open_without_rows_key](../../../functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key.md)
- [move_default_view_folder_open_without_rows_to_special_folder_bootstrap](../../../functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap.md)