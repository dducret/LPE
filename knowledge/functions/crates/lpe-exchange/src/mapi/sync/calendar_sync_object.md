---
type: Rust Function
title: calendar_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L959-L1036
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/event_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_sync_object_projects_canonical_attachment_presence
---

# Signature

`fn calendar_sync_object( event: &crate::mapi_store::MapiEvent, reminder: Option<&lpe_storage::ClientReminder>, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [versioned_event_property_value_with_reminder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [event_size](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/event_size.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [calendar_sync_object_projects_canonical_attachment_presence](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_sync_object_projects_canonical_attachment_presence.md)