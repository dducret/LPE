---
type: Rust Function
title: task_matches_report
resource: crates/lpe-dav/src/report.rs#L143-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/task_href
  - functions/crates/lpe-dav/src/report/normalize_time_range_value
  - functions/crates/lpe-dav/src/serialize/format_ical_timestamp
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn task_matches_report(task: &DavTask, filter: &ReportFilter) -> bool`

# Calls

- [task_href](../../../../../functions/crates/lpe-dav/src/paths/task_href.md)
- [normalize_time_range_value](../../../../../functions/crates/lpe-dav/src/report/normalize_time_range_value.md)
- [format_ical_timestamp](../../../../../functions/crates/lpe-dav/src/serialize/format_ical_timestamp.md)

# Called by

- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)