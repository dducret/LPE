---
type: Rust Function
title: rop_get_receive_folder_table_response
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L92-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_receive_folder_table_response
---

# Signature

`pub(in crate::mapi) fn rop_get_receive_folder_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [write_standard_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row.md)

# Called by

- [get_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_receive_folder_table_response.md)