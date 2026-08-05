---
type: Rust Function
title: serialize_deleted_items_content_row
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L93-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
---

# Signature

`pub(super) fn serialize_deleted_items_content_row( row: DeletedItemsContentRow<'_>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [message_for_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [serialize_mapi_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid.md)
- [serialize_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_mailbox_guid.md)
- [serialize_versioned_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)