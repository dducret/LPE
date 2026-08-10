---
type: Rust Function
title: deleted_items_category_values
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L232-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
---

# Signature

`fn deleted_items_category_values( row: &DeletedItemsContentRow<'_>, property_tag: u32, ) -> Vec<String>`

# Calls

- [category_values_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_for_email.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)

# Called by

- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)