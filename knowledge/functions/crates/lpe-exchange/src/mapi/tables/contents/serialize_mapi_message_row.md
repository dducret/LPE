---
type: Rust Function
title: serialize_mapi_message_row
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L109-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_mapi_message_row( message: &MapiMessage, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_row_with_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity.md)

# Called by

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)