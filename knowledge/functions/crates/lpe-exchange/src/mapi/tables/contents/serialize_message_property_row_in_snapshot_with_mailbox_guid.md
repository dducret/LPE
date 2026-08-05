---
type: Rust Function
title: serialize_message_property_row_in_snapshot_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L157-L176
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_property_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_table_row_flags_absent_deadline_expiry_and_recall_times
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity
---

# Signature

`pub(in crate::mapi) fn serialize_message_property_row_in_snapshot_with_mailbox_guid( email: &JmapEmail, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [message_for_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [serialize_mapi_message_property_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_property_row_with_mailbox_guid.md)
- [serialize_message_property_row_with_durable_identity_and_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_with_durable_identity_and_mailbox_guid.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [serialize_deleted_items_content_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_property_row.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [message_table_row_flags_absent_deadline_expiry_and_recall_times](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_table_row_flags_absent_deadline_expiry_and_recall_times.md)
- [normal_contents_property_row_uses_durable_message_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity.md)