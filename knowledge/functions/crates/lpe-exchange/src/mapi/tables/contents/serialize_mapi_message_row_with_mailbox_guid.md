---
type: Rust Function
title: serialize_mapi_message_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L120-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row
---

# Signature

`pub(in crate::mapi) fn serialize_mapi_message_row_with_mailbox_guid( message: &MapiMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)

# Called by

- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [serialize_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row.md)
- [serialize_search_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row.md)