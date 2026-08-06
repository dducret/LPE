---
type: Rust Method
title: reminder_for_source
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1167-L1180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row
---

# Signature

`pub(crate) fn reminder_for_source( &self, source_type: &str, source_id: Uuid, ) -> Option<&ClientReminder>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [persisted_object_property_delete_is_idempotent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [serialize_event_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [serialize_search_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row.md)