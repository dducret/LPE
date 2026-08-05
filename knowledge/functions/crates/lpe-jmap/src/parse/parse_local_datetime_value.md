---
type: Rust Function
title: parse_local_datetime_value
resource: crates/lpe-jmap/src/parse.rs#L116-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime
  called_by:
  - functions/crates/lpe-jmap/src/calendar/event_matches_filter
---

# Signature

`pub(crate) fn parse_local_datetime_value(value: &str) -> Result<String>`

# Calls

- [parse_local_datetime](../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime.md)

# Called by

- [event_matches_filter](../../../../../functions/crates/lpe-jmap/src/calendar/event_matches_filter.md)