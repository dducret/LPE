---
type: Rust Function
title: parse_first_property_object_string
resource: crates/lpe-jmap/src/parse.rs#L35-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_location
---

# Signature

`pub(crate) fn parse_first_property_object_string( value: Option<&Value>, property_name: &str, field_name: &str, ) -> Result<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_calendar_location](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_location.md)