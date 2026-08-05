---
type: Rust Function
title: parse_organizer
resource: crates/lpe-dav/src/parse.rs#L273-L280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/property_parameter
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_ical
---

# Signature

`fn parse_organizer(left: &str, value: &str) -> Result<Option<CalendarOrganizerMetadata>>`

# Calls

- [property_parameter](../../../../../functions/crates/lpe-dav/src/parse/property_parameter.md)

# Called by

- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)