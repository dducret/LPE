---
type: Python Function
title: build_scope_for
resource: tools/rca_outlook_trace_summary.py#L3729-L3738
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/format_build_dirty
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_build_scope_identifies_current_clean_and_dirty_builds
---

# Signature

`def build_scope_for( build_commit: object, git_dirty: object, current_build: str | None ) -> str:`

# Calls

- [format_build_dirty](../../../functions/tools/rca_outlook_trace_summary/format_build_dirty.md)

# Called by

- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_build_scope_identifies_current_clean_and_dirty_builds](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_build_scope_identifies_current_clean_and_dirty_builds.md)