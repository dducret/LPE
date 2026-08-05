---
type: Rust Function
title: serialize_object_property_row_with_custom
resource: crates/lpe-exchange/src/mapi/rop.rs#L569-L594
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
---

# Signature

`fn serialize_object_property_row_with_custom( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, columns: &[u32], custom_values: &HashMap<u32, Vec<u8>>, ) -> Vec<u8>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [get_properties_specific_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)