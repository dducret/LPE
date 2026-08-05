---
type: Rust Function
title: persisted_object_property_delete_is_idempotent
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L661-L688
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/persisted_message_delete_is_best_effort
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`fn persisted_object_property_delete_is_idempotent( object: Option<&MapiObject>, property_tags: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> bool`

# Calls

- [persisted_message_delete_is_best_effort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/persisted_message_delete_is_best_effort.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [reminder_for_source](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)