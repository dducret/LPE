---
type: Rust Function
title: inbox_getprops_captured_unpersisted_folder_values_are_absent
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L3725-L3861
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
---

# Signature

`fn inbox_getprops_captured_unpersisted_folder_values_are_absent()`

# Calls

- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [rop_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)