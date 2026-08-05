---
type: Rust Function
title: parse_create_event_input
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L112-L166
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_parts
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants
  - functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn parse_create_event_input( principal: &AccountPrincipal, request: &str, ) -> Result<UpsertClientEventInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [ews_datetime_parts](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_parts.md)
- [ews_duration_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [parse_event_participants](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants.md)
- [requested_time_zone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone.md)
- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)