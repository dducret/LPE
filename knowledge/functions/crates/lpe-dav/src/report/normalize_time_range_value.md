---
type: Rust Function
title: normalize_time_range_value
resource: crates/lpe-dav/src/report.rs#L183-L189
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/report/event_matches_report
  - functions/crates/lpe-dav/src/report/task_matches_report
---

# Signature

`fn normalize_time_range_value(value: &str) -> Option<String>`

# Called by

- [event_matches_report](../../../../../functions/crates/lpe-dav/src/report/event_matches_report.md)
- [task_matches_report](../../../../../functions/crates/lpe-dav/src/report/task_matches_report.md)