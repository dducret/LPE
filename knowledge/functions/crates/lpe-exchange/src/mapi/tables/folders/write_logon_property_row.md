---
type: Rust Function
title: write_logon_property_row
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L748-L774
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
---

# Signature

`pub(in crate::mapi) fn write_logon_property_row( response: &mut Vec<u8>, principal: &AccountPrincipal, columns: &[u32], )`

# Calls

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [write_standard_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row.md)
- [serialize_logon_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)