---
type: Rust Function
title: populate_special_message_named_property_definitions
resource: crates/lpe-exchange/src/mapi/sync.rs#L362-L386
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_property_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/special_message_with_named_property_definitions
---

# Signature

`fn populate_special_message_named_property_definitions( object: &mut mapi_mailstore::SpecialMessageSyncFact, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [property_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [named_property_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_property_for_id.md)
- [fast_transfer_named_property_for_message_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [special_message_with_named_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_message_with_named_property_definitions.md)