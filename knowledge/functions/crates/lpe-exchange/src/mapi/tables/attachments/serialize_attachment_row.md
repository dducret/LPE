---
type: Rust Function
title: serialize_attachment_row
resource: crates/lpe-exchange/src/mapi/tables/attachments.rs#L3-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_inline
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_rows_use_by_value_method
  - functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_message_attachment_example_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_inline_image_example_columns
---

# Signature

`pub(in crate::mapi) fn serialize_attachment_row( attachment: &MapiAttachment, columns: &[u32], ) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [attachment_file_extension](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension.md)
- [attachment_method_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [attachment_is_inline](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_inline.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [attachment_rows_use_by_value_method](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_rows_use_by_value_method.md)
- [attachment_row_projects_microsoft_message_attachment_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_message_attachment_example_columns.md)
- [attachment_row_projects_microsoft_inline_image_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_inline_image_example_columns.md)