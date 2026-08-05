---
type: Rust Function
title: serialize_pending_attachment_row
resource: crates/lpe-exchange/src/mapi/tables/attachments.rs#L54-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_content_id
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_hidden
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value_from_metadata
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

`pub(in crate::mapi) fn serialize_pending_attachment_row( attach_num: u32, properties: &HashMap<u32, MapiValue>, data: &[u8], columns: &[u32], ) -> Vec<u8>`

# Calls

- [pending_attachment_file_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name.md)
- [pending_attachment_media_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type.md)
- [pending_attachment_content_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_content_id.md)
- [pending_attachment_hidden](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_hidden.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [attachment_file_extension](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_file_extension.md)
- [attachment_method_value_from_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_method_value_from_metadata.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [attachment_row_projects_microsoft_inline_image_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/attachment_row_projects_microsoft_inline_image_example_columns.md)