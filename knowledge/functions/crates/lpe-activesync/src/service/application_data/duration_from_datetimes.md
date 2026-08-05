---
type: Rust Function
title: duration_from_datetimes
resource: crates/lpe-activesync/src/service/application_data.rs#L277-L283
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/application_data/parse_compact_datetime
  - functions/crates/lpe-activesync/src/service/application_data/date_time_to_minutes
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
---

# Signature

`fn duration_from_datetimes(start: &str, end: &str) -> Result<i32>`

# Calls

- [parse_compact_datetime](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_compact_datetime.md)
- [date_time_to_minutes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/date_time_to_minutes.md)

# Called by

- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)