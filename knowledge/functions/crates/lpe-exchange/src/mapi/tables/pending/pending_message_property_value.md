---
type: Rust Function
title: pending_message_property_value
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L367-L424
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key
  - functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_associated_message_property_value
---

# Signature

`pub(in crate::mapi) fn pending_message_property_value( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [pending_message_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [pending_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_change_number.md)
- [pending_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_search_key.md)
- [mailbox_owner_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id.md)

# Called by

- [serialize_pending_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row.md)
- [pending_associated_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_associated_message_property_value.md)