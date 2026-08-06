---
type: Rust Function
title: meeting_response_event_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L739-L835
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_response_classes_map_to_partstat
---

# Signature

`pub(in crate::mapi) fn meeting_response_event_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &AccessibleEvent, properties: &HashMap<u32, MapiValue>, ) -> Result<Option<UpsertClientEventInput>>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)

# Called by

- [validate_staged_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)
- [mapi_over_http_calendar_meeting_response_classes_map_to_partstat](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_response_classes_map_to_partstat.md)