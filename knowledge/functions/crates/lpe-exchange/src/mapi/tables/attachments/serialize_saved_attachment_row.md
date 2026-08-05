---
type: Rust Function
title: serialize_saved_attachment_row
resource: crates/lpe-exchange/src/mapi/tables/attachments.rs#L118-L157
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_inline_image_example_columns
---

# Signature

`pub(in crate::mapi) fn serialize_saved_attachment_row( attach_num: u32, file_reference: &str, file_name: &str, media_type: &str, disposition: Option<&str>, content_id: Option<&str>, size_octets: u64, columns: &[u32], ) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [attachment_file_extension](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [attachment_row_projects_microsoft_inline_image_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_inline_image_example_columns.md)