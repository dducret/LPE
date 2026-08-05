---
type: Rust Function
title: category_values_for_email
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L29-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_from_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values
---

# Signature

`pub(super) fn category_values_for_email(email: &JmapEmail, property_tag: u32) -> Vec<String>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [category_values_from_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_values_from_mapi_value.md)

# Called by

- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [deleted_items_category_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values.md)