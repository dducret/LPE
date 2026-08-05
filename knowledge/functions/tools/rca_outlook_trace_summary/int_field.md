---
type: Python Function
title: int_field
resource: tools/rca_outlook_trace_summary.py#L1036-L1043
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap
  - functions/tools/rca_outlook_trace_summary/record_common_views_fai_transfer_summary
  - functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup
---

# Signature

`def int_field(fields: dict[str, Any], key: str) -> int:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [record_broad_ipm_configuration_row_count_gap](../../../functions/tools/rca_outlook_trace_summary/record_broad_ipm_configuration_row_count_gap.md)
- [record_common_views_fai_transfer_summary](../../../functions/tools/rca_outlook_trace_summary/record_common_views_fai_transfer_summary.md)
- [record_post_visible_release_followup](../../../functions/tools/rca_outlook_trace_summary/record_post_visible_release_followup.md)