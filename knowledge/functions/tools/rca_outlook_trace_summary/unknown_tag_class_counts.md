---
type: Python Function
title: unknown_tag_class_counts
resource: tools/rca_outlook_trace_summary.py#L2239-L2243
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/classify_unknown_getprops_tag
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_tag_classes_group_unconfirmed_ranges
---

# Signature

`def unknown_tag_class_counts(counter: Counter[str]) -> Counter[str]:`

# Calls

- [classify_unknown_getprops_tag](../../../functions/tools/rca_outlook_trace_summary/classify_unknown_getprops_tag.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_unknown_getprops_tag_classes_group_unconfirmed_ranges](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_unknown_getprops_tag_classes_group_unconfirmed_ranges.md)