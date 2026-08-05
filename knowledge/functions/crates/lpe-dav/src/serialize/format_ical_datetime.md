---
type: Rust Function
title: format_ical_datetime
resource: crates/lpe-dav/src/serialize.rs#L97-L99
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/report/event_matches_report
  - functions/crates/lpe-dav/src/serialize/serialize_ical
---

# Signature

`pub(crate) fn format_ical_datetime(date: &str, time: &str) -> String`

# Called by

- [event_matches_report](../../../../../functions/crates/lpe-dav/src/report/event_matches_report.md)
- [serialize_ical](../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)