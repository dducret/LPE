---
type: Rust Function
title: serialize_versioned_event_row_with_reminder_and_attachments
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L154-L185
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row
---

# Signature

`pub(in crate::mapi) fn serialize_versioned_event_row_with_reminder_and_attachments( event: &crate::mapi_store::MapiEvent, reminder: Option<&lpe_storage::ClientReminder>, has_attachments: bool, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_event_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property.md)
- [serialize_versioned_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row.md)