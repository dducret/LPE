---
type: Rust Function
title: serialize_mapi_message_property_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L144-L155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn serialize_mapi_message_property_row_with_mailbox_guid( message: &MapiMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_property_row_with_durable_identity_and_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [serialize_message_property_row_in_snapshot_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)