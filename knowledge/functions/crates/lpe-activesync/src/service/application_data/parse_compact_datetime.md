---
type: Rust Function
title: parse_compact_datetime
resource: crates/lpe-activesync/src/service/application_data.rs#L258-L275
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
  - functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes
---

# Signature

`fn parse_compact_datetime(value: &str) -> Result<(String, String)>`

# Called by

- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)
- [duration_from_datetimes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes.md)