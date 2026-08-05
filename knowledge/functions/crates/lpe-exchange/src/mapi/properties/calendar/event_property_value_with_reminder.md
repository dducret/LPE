---
type: Rust Function
title: event_property_value_with_reminder
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L15-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments
---

# Signature

`pub(in crate::mapi) fn event_property_value_with_reminder( event: &AccessibleEvent, item_id: u64, folder_id: u64, property_tag: u32, reminder: Option<&lpe_storage::ClientReminder>, ) -> Option<MapiValue>`

# Calls

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)

# Called by

- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [persisted_object_property_delete_is_idempotent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent.md)
- [event_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [serialize_event_row_with_reminder_and_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments.md)