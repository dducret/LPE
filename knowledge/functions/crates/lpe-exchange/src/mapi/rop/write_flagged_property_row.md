---
type: Rust Function
title: write_flagged_property_row
resource: crates/lpe-exchange/src/mapi/rop.rs#L777-L821
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_error
  - functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
---

# Signature

`fn write_flagged_property_row( response: &mut Vec<u8>, object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, columns: &[u32], unsupported_tags: &[u32], size_limited_properties: &[bool], custom_values: &HashMap<u32, Vec<u8>>, )`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [get_properties_specific_typed_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)
- [write_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [write_flagged_property_error](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_error.md)
- [flagged_property_error_code](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)