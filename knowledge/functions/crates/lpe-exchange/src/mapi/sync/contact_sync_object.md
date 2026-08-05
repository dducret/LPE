---
type: Rust Function
title: contact_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L454-L513
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn contact_sync_object( contact: &crate::mapi_store::MapiContact, mailbox_guid: Uuid, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [contact_property_value_with_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [contact_size](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_size.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)