---
type: Rust Module
title: report
resource: crates/lpe-dav/src/report.rs#L1-L189
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-storage-accessiblecontact-accessibleevent-davtask
  - external/crate-paths-contact-href-event-href-task-href-serialize-format-ical-datetime-format-ical-timestamp
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [ReportFilter](../../../../classes/crates/lpe-dav/src/report/ReportFilter.md)
- [parse_report_filter](../../../../functions/crates/lpe-dav/src/report/parse_report_filter.md)
- [xml_tag_values](../../../../functions/crates/lpe-dav/src/report/xml_tag_values.md)
- [xml_text_match_values](../../../../functions/crates/lpe-dav/src/report/xml_text_match_values.md)
- [xml_attribute_value](../../../../functions/crates/lpe-dav/src/report/xml_attribute_value.md)
- [contact_matches_report](../../../../functions/crates/lpe-dav/src/report/contact_matches_report.md)
- [event_matches_report](../../../../functions/crates/lpe-dav/src/report/event_matches_report.md)
- [task_matches_report](../../../../functions/crates/lpe-dav/src/report/task_matches_report.md)
- [normalize_time_range_value](../../../../functions/crates/lpe-dav/src/report/normalize_time_range_value.md)

# Imports

- `anyhow::Result`
- `lpe_storage::{AccessibleContact, AccessibleEvent, DavTask}`
- `crate::{
    paths::{contact_href, event_href, task_href},
    serialize::{format_ical_datetime, format_ical_timestamp},
}`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)