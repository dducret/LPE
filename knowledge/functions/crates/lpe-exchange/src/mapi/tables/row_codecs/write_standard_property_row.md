---
type: Rust Function
title: write_standard_property_row
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L3-L6
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
---

# Signature

`pub(in crate::mapi) fn write_standard_property_row(response: &mut Vec<u8>, values: &[u8])`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [rop_get_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response.md)
- [rop_expand_row_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [write_logon_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row.md)
- [standard_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)