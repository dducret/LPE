---
type: Rust Function
title: serialize_deleted_items_content_property_row
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L112-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids
---

# Signature

`pub(super) fn serialize_deleted_items_content_property_row( row: DeletedItemsContentRow<'_>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_property_row_in_snapshot_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_property_row_in_snapshot_with_mailbox_guid.md)
- [serialize_versioned_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row.md)
- [write_query_rows_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [categorized_and_deleted_message_rows_keep_long_term_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids.md)