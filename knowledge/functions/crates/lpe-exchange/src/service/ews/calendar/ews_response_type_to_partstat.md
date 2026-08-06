---
type: Rust Function
title: ews_response_type_to_partstat
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L618-L622
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/ews_types/EwsResponseType/partstat
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_attendee
---

# Signature

`fn ews_response_type_to_partstat(response_type: &Option<String>) -> String`

# Calls

- [partstat](../../../../../../../functions/crates/lpe-exchange/src/ews_types/EwsResponseType/partstat.md)

# Called by

- [parse_attendee](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_attendee.md)