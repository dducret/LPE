---
type: Rust Function
title: stage_event_property_deletions
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L378-L445
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_property_is_server_managed
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_clearable_event_property_deletion
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_reminder_is_active
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(super) fn stage_event_property_deletions( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Result<Vec<(usize, u32, u32)>>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [event_handle_is_writable](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable.md)
- [reminder_for_source](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [event_property_is_server_managed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_property_is_server_managed.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [stage_clearable_event_property_deletion](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_clearable_event_property_deletion.md)
- [staged_event_reminder_is_active](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_reminder_is_active.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)