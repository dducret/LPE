---
type: Rust Function
title: parse_local_datetime
resource: crates/lpe-jmap/src/parse.rs#L96-L114
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
  - functions/crates/lpe-jmap/src/mail/values/validate_email_submission_query
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime_value
  - functions/crates/lpe-jmap/src/validation/validate_calendar_event_filter
---

# Signature

`pub(crate) fn parse_local_datetime(value: &str) -> Result<(String, String)>`

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
- [validate_email_submission_query](../../../../../functions/crates/lpe-jmap/src/mail/values/validate_email_submission_query.md)
- [parse_local_datetime_value](../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime_value.md)
- [validate_calendar_event_filter](../../../../../functions/crates/lpe-jmap/src/validation/validate_calendar_event_filter.md)