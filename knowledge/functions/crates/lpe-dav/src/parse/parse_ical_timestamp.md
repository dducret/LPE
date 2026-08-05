---
type: Rust Function
title: parse_ical_timestamp
resource: crates/lpe-dav/src/parse.rs#L225-L237
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/parse_ical_datetime
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_vtodo
---

# Signature

`fn parse_ical_timestamp(value: &str) -> Result<String>`

# Calls

- [parse_ical_datetime](../../../../../functions/crates/lpe-dav/src/parse/parse_ical_datetime.md)

# Called by

- [parse_vtodo](../../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)