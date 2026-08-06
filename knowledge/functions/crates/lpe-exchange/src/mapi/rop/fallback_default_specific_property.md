---
type: Rust Function
title: fallback_default_specific_property
resource: crates/lpe-exchange/src/mapi/rop.rs#L631-L748
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/event_object_property_is_deleted
  - functions/crates/lpe-exchange/src/mapi/rop/contact_properties/contact_object_property_is_deleted
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_object_property_is_deleted
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property
  - functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code
---

# Signature

`fn fallback_default_specific_property( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, tag: u32, ) -> bool`

# Calls

- [event_object_property_is_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/event_object_property_is_deleted.md)
- [contact_object_property_is_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/contact_properties/contact_object_property_is_deleted.md)
- [navigation_shortcut_object_property_is_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_object_property_is_deleted.md)
- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [get_properties_specific_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)
- [email_property_value_with_durable_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity.md)
- [search_folder_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [write_property_default](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [modeled_zero_or_default_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property.md)
- [associated_config_modeled_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property.md)

# Called by

- [unsupported_specific_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)
- [flagged_property_error_code](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code.md)