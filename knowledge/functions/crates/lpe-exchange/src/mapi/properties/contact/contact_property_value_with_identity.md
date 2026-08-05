---
type: Rust Function
title: contact_property_value_with_identity
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L19-L198
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_address_book_provider_email_list
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value
  - functions/crates/lpe-exchange/src/mapi/properties/contact/outlook_contact_source_empty_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_url_by_label
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key
  - functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row
---

# Signature

`pub(in crate::mapi) fn contact_property_value_with_identity( contact: &AccessibleContact, item_id: u64, folder_id: u64, mailbox_guid: Uuid, identity: Option<&crate::store::MapiIdentityRecord>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [contact_address_book_provider_email_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_address_book_provider_email_list.md)
- [contact_email_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_email_value.md)
- [outlook_contact_source_empty_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/outlook_contact_source_empty_value.md)
- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [contact_url_by_label](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_url_by_label.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [contact_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [format_contact_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [contact_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value.md)
- [contact_entry_id_is_private_message_entry_id_not_a_sync_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_entry_id_is_private_message_entry_id_not_a_sync_key.md)
- [contact_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)
- [serialize_mapi_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row.md)