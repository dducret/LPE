---
type: Rust Function
title: parse_ical
resource: crates/lpe-dav/src/parse.rs#L61-L144
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/parse/unfolded_lines
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-dav/src/parse/text_unescape
  - functions/crates/lpe-dav/src/parse/parse_ical_datetime
  - functions/crates/lpe-dav/src/parse/property_parameter
  - functions/crates/lpe-dav/src/parse/parse_ical_duration
  - functions/crates/lpe-dav/src/parse/parse_organizer
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn parse_ical( id: Uuid, account_id: Uuid, body: &[u8], ) -> Result<UpsertClientEventInput>`

# Calls

- [unfolded_lines](../../../../../functions/crates/lpe-dav/src/parse/unfolded_lines.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [text_unescape](../../../../../functions/crates/lpe-dav/src/parse/text_unescape.md)
- [parse_ical_datetime](../../../../../functions/crates/lpe-dav/src/parse/parse_ical_datetime.md)
- [property_parameter](../../../../../functions/crates/lpe-dav/src/parse/property_parameter.md)
- [parse_ical_duration](../../../../../functions/crates/lpe-dav/src/parse/parse_ical_duration.md)
- [parse_organizer](../../../../../functions/crates/lpe-dav/src/parse/parse_organizer.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [calendar_attendee_labels](../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [serialize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)