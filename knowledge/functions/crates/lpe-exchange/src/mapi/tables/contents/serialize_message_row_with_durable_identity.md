---
type: Rust Function
title: serialize_message_row_with_durable_identity
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L101-L107
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row
---

# Signature

`pub(in crate::mapi) fn serialize_message_row_with_durable_identity( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)

# Called by

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row.md)
- [serialize_mapi_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row.md)