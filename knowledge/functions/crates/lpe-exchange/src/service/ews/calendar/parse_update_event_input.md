---
type: Rust Function
title: parse_update_event_input
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L168-L266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants
  - functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
  - functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text
  - functions/crates/lpe-exchange/src/service/ews/fields/String/emptystringfallback/if_empty
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`pub(in crate::service) fn parse_update_event_input( principal: &AccountPrincipal, existing: &AccessibleEvent, request: &str, ) -> Result<UpsertClientEventInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [ews_duration_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [parse_event_participants](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants.md)
- [requested_time_zone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone.md)
- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)
- [deleted_or_updated_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text.md)
- [if_empty](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/String/emptystringfallback/if_empty.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)