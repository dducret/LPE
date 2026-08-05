---
type: Rust Function
title: write_category_instance_value
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L516-L524
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row
---

# Signature

`pub(super) fn write_category_instance_value(row: &mut Vec<u8>, property_tag: u32, value: &str)`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row.md)
- [serialize_categorized_deleted_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row.md)