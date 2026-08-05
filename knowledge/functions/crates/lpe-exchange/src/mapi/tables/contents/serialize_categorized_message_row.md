---
type: Rust Function
title: serialize_categorized_message_row
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L238-L256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row
---

# Signature

`pub(super) fn serialize_categorized_message_row( email: &JmapEmail, durable_identity: Option<&crate::store::MapiIdentityRecord>, mailbox_guid: Uuid, columns: &[u32], category_property_tag: u32, category_value: &str, instance_num: u32, ) -> Vec<u8>`

# Calls

- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)

# Called by

- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [serialize_categorized_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row.md)