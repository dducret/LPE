---
type: Rust Function
title: event_property_value_with_reminder_and_mailbox_guid
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L32-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_backs_outlook_table_identity_and_status_columns
---

# Signature

`pub(in crate::mapi) fn event_property_value_with_reminder_and_mailbox_guid( event: &AccessibleEvent, item_id: u64, folder_id: u64, property_tag: u32, reminder: Option<&lpe_storage::ClientReminder>, mailbox_guid: Option<Uuid>, ) -> Option<MapiValue>`

# Calls

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)

# Called by

- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [calendar_projection_backs_outlook_table_identity_and_status_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_backs_outlook_table_identity_and_status_columns.md)