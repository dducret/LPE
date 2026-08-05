---
type: Rust Function
title: email_property_value_with_durable_identity
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L208-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/tables/contents/message_table_property_is_present
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
---

# Signature

`pub(in crate::mapi) fn email_property_value_with_durable_identity( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)

# Called by

- [fallback_default_specific_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [message_table_property_is_present](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/message_table_property_is_present.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)