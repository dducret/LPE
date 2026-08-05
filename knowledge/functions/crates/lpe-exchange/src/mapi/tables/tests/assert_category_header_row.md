---
type: Rust Function
title: assert_category_header_row
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L2899-L2936
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata
---

# Signature

`fn assert_category_header_row( row: &[u8], category: &str, content_count: u32, unread_count: u32, row_type: u32, )`

# Calls

- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)

# Called by

- [categorized_keywords_project_multivalue_instances_and_table_row_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata.md)