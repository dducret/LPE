---
type: Python Function
title: default_view_open_is_special_folder_bootstrap
resource: tools/rca_outlook_trace_summary.py#L1590-L1610
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
---

# Signature

`def default_view_open_is_special_folder_bootstrap( fields: dict[str, Any], base_key: str, setprops_context: str ) -> bool:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)