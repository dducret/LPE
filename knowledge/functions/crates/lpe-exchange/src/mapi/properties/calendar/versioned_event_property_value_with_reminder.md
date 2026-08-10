---
type: Rust Function
title: versioned_event_property_value_with_reminder
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L52-L67
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row_with_reminder_and_attachments
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row
---

# Signature

`pub(in crate::mapi) fn versioned_event_property_value_with_reminder( event: &MapiEvent, property_tag: u32, reminder: Option<&lpe_storage::ClientReminder>, ) -> Option<MapiValue>`

# Calls

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)

# Called by

- [calendar_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)
- [restriction_matches_event_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid.md)
- [serialize_versioned_event_row_with_reminder_and_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row_with_reminder_and_attachments.md)
- [deleted_items_category_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values.md)
- [serialize_categorized_deleted_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row.md)