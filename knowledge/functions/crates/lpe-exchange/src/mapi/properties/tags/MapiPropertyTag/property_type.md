---
type: Rust Method
title: property_type
resource: crates/lpe-exchange/src/mapi/properties/tags.rs#L21-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_are_supported
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_have_known_wire_types
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/format_unknown_wire_type_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_guid_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_all_response_tag
  - functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
---

# Signature

`pub(in crate::mapi) fn property_type(self) -> Option<MapiPropertyType>`

# Calls

- [property_type_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [is_custom_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [property_tags_are_supported](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_are_supported.md)
- [property_tags_have_known_wire_types](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/property_tags_have_known_wire_types.md)
- [set_columns_property_tags_are_valid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_property_tags_are_valid.md)
- [format_unknown_wire_type_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/format_unknown_wire_type_property_tags.md)
- [normal_message_defaulted_column_detail](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail.md)
- [wlink_guid_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_guid_property_value.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_mapi_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [property_is_unsupported_for_object](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [get_properties_all_response_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_all_response_tag.md)
- [write_category_instance_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/write_category_instance_value.md)
- [write_query_rows_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_value.md)
- [write_property_default](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)