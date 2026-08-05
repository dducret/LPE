---
type: Rust Function
title: event_property_value_with_optional_version
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L49-L156
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/event_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_name
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_email
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_busy_status
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_global_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
---

# Signature

`fn event_property_value_with_optional_version( event: &AccessibleEvent, item_id: u64, folder_id: u64, property_tag: u32, reminder: Option<&lpe_storage::ClientReminder>, version: Option<&lpe_storage::MapiEventVersion>, source_key: Option<&[u8]>, ) -> Option<MapiValue>`

# Calls

- [event_reminder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [event_start_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)
- [event_end_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [event_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/event_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [calendar_organizer_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_name.md)
- [calendar_organizer_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_email.md)
- [calendar_display_to](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to.md)
- [calendar_optional_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees.md)
- [calendar_all_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees.md)
- [calendar_required_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees.md)
- [appointment_busy_status](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_busy_status.md)
- [appointment_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration.md)
- [appointment_state_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags.md)
- [calendar_time_zone_struct](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct.md)
- [calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key.md)
- [calendar_time_zone_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition.md)
- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)
- [calendar_global_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_global_object_id.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)