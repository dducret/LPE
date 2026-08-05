---
type: Rust Function
title: serialize_message_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L136-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row
---

# Signature

`pub(in crate::mapi) fn serialize_message_row_with_mailbox_guid( email: &JmapEmail, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)

# Called by

- [serialize_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row.md)