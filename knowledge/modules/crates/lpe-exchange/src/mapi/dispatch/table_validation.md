---
type: Rust Module
title: table_validation
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L1-L611
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [property_tags_are_supported](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_are_supported.md)
- [property_tags_have_known_wire_types](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_have_known_wire_types.md)
- [restrict_supported_on_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/restrict_supported_on_object.md)
- [sort_table_request_is_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid.md)
- [sort_order_is_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_order_is_valid.md)
- [maximum_category_sort_order_is_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/maximum_category_sort_order_is_valid.md)
- [get_attachment_table_flags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/get_attachment_table_flags_are_valid.md)
- [hierarchy_table_flags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/hierarchy_table_flags_are_valid.md)
- [contents_table_flags_error](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/contents_table_flags_error.md)
- [open_attachment_flags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/open_attachment_flags_are_valid.md)
- [SaveDisposition](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/table_validation/SaveDisposition.md)
- [save_disposition](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition.md)
- [save_flags_are_supported](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported.md)
- [table_async_flags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/table_async_flags_are_valid.md)
- [set_columns_request_is_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid.md)
- [set_columns_request_is_valid_for_rule_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table.md)
- [set_columns_property_tags_are_valid](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid.md)
- [set_columns_property_tags_are_valid_for_rule_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid_for_rule_table.md)
- [format_unknown_wire_type_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/format_unknown_wire_type_property_tags.md)
- [set_columns_accepts_multi_value_instance_property_types](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_accepts_multi_value_instance_property_types.md)
- [set_columns_rejects_microsoft_invalid_column_property_types](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_rejects_microsoft_invalid_column_property_types.md)
- [rule_table_set_columns_accepts_documented_rule_complex_types](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/rule_table_set_columns_accepts_documented_rule_complex_types.md)
- [set_columns_request_validation_matches_microsoft_flags_and_count](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_validation_matches_microsoft_flags_and_count.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request.md)
- [restrict_flags_validation_matches_microsoft_async_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/restrict_flags_validation_matches_microsoft_async_flags.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-2.md)
- [restrict_support_matches_microsoft_table_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/restrict_support_matches_microsoft_table_scope.md)
- [sort_table_request_validation_matches_microsoft_bounds](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_validation_matches_microsoft_bounds.md)
- [request_with_orders](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request_with_orders.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-3.md)
- [get_attachment_table_flags_match_microsoft_message_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/get_attachment_table_flags_match_microsoft_message_values.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-4.md)
- [hierarchy_table_flags_match_microsoft_folder_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/hierarchy_table_flags_match_microsoft_folder_values.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-5.md)
- [contents_table_flags_match_microsoft_folder_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/contents_table_flags_match_microsoft_folder_values.md)
- [open_attachment_flags_match_microsoft_message_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/open_attachment_flags_match_microsoft_message_values.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-6.md)
- [save_flags_match_microsoft_message_and_attachment_combinations](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_match_microsoft_message_and_attachment_combinations.md)
- [request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/request-7.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)