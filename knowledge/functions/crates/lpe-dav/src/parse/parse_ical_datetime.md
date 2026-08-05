---
type: Rust Function
title: parse_ical_datetime
resource: crates/lpe-dav/src/parse.rs#L206-L223
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/parse/parse_ical_timestamp
---

# Signature

`fn parse_ical_datetime(value: &str) -> Result<(String, String)>`

# Called by

- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [parse_ical_timestamp](../../../../../functions/crates/lpe-dav/src/parse/parse_ical_timestamp.md)