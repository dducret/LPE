---
type: Rust Function
title: serialize_search_content_row
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L127-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn serialize_search_content_row( row: SearchContentRow<'_>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], reminder_projection: bool, ) -> Vec<u8>`

# Calls

- [serialize_mapi_message_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row_with_mailbox_guid.md)
- [serialize_reminder_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row.md)
- [reminder_for_source](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [serialize_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)