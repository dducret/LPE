---
type: Rust Function
title: property_parameter
resource: crates/lpe-dav/src/parse.rs#L272-L281
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/text_unescape
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/parse/parse_organizer
  - functions/crates/lpe-dav/src/parse/parse_attendee
---

# Signature

`fn property_parameter(left: &str, name: &str) -> Option<String>`

# Calls

- [text_unescape](../../../../../functions/crates/lpe-dav/src/parse/text_unescape.md)

# Called by

- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [parse_organizer](../../../../../functions/crates/lpe-dav/src/parse/parse_organizer.md)
- [parse_attendee](../../../../../functions/crates/lpe-dav/src/parse/parse_attendee.md)