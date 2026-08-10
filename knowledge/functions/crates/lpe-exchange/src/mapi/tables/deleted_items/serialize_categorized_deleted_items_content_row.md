---
type: Rust Function
title: serialize_categorized_deleted_items_content_row
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L255-L284
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
---

# Signature

`fn serialize_categorized_deleted_items_content_row( row: DeletedItemsContentRow<'_>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], category_property_tag: u32, category_value: &str, instance_num: u32, ) -> Vec<u8>`

# Calls

- [serialize_categorized_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_categorized_message_row.md)
- [message_for_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [serialize_categorized_deleted_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row.md)

# Called by

- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)