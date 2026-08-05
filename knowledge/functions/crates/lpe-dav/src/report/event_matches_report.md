---
type: Rust Function
title: event_matches_report
resource: crates/lpe-dav/src/report.rs#L106-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/event_href
  - functions/crates/lpe-dav/src/serialize/format_ical_datetime
  - functions/crates/lpe-dav/src/report/normalize_time_range_value
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn event_matches_report(event: &AccessibleEvent, filter: &ReportFilter) -> bool`

# Calls

- [event_href](../../../../../functions/crates/lpe-dav/src/paths/event_href.md)
- [format_ical_datetime](../../../../../functions/crates/lpe-dav/src/serialize/format_ical_datetime.md)
- [normalize_time_range_value](../../../../../functions/crates/lpe-dav/src/report/normalize_time_range_value.md)

# Called by

- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)