---
type: Rust Function
title: serialize_category_header_row
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L480-L514
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
---

# Signature

`pub(super) fn serialize_category_header_row( category_id: u64, value: &str, leaf_count: usize, unread_count: usize, category_property_tag: u32, expanded: bool, columns: &[u32], ) -> Vec<u8>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_category_instance_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)