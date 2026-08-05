---
type: Rust Function
title: serialize_categorized_deleted_event_row
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L278-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row
---

# Signature

`fn serialize_categorized_deleted_event_row( event: &crate::mapi_store::MapiEvent, columns: &[u32], category_property_tag: u32, category_value: &str, instance_num: u32, ) -> Vec<u8>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_category_instance_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_categorized_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row.md)