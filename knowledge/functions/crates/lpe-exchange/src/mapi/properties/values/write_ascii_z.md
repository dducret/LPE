---
type: Rust Function
title: write_ascii_z
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L380-L387
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string8
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_string8
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
---

# Signature

`pub(in crate::mapi) fn write_ascii_z(row: &mut Vec<u8>, value: &str)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)
- [write_nspi_multi_string8](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string8.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_multi_string8](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_string8.md)
- [modify_recipients_accepts_microsoft_message_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)