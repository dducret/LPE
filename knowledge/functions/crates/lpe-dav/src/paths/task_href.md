---
type: Rust Function
title: task_href
resource: crates/lpe-dav/src/paths.rs#L59-L61
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/propfind/task_resource_entry
  - functions/crates/lpe-dav/src/propfind/task_report_entry
  - functions/crates/lpe-dav/src/report/task_matches_report
---

# Signature

`pub(crate) fn task_href(collection_id: &str, id: Uuid) -> String`

# Called by

- [task_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)
- [task_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_report_entry.md)
- [task_matches_report](../../../../../functions/crates/lpe-dav/src/report/task_matches_report.md)