---
type: Rust Function
title: serialize_message_property_row_with_durable_identity_and_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L178-L225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/message_table_property_is_present
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_property_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid
---

# Signature

`fn serialize_message_property_row_with_durable_identity_and_mailbox_guid( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, mailbox_guid: Option<Uuid>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [message_table_property_is_present](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/message_table_property_is_present.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [write_query_rows_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [serialize_mapi_message_property_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_property_row_with_mailbox_guid.md)
- [serialize_message_property_row_in_snapshot_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid.md)