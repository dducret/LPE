---
type: Rust Function
title: message_table_property_is_present
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L227-L236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid
---

# Signature

`fn message_table_property_is_present( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, property_tag: u32, ) -> bool`

# Calls

- [email_property_value_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value_with_durable_identity.md)

# Called by

- [serialize_message_property_row_with_durable_identity_and_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid.md)