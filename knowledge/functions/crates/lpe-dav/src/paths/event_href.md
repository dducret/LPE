---
type: Rust Function
title: event_href
resource: crates/lpe-dav/src/paths.rs#L55-L57
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/propfind/event_resource_entry
  - functions/crates/lpe-dav/src/propfind/event_report_entry
  - functions/crates/lpe-dav/src/report/event_matches_report
---

# Signature

`pub(crate) fn event_href(collection_id: &str, id: Uuid) -> String`

# Called by

- [event_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [event_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_report_entry.md)
- [event_matches_report](../../../../../functions/crates/lpe-dav/src/report/event_matches_report.md)