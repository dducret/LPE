---
type: Rust Function
title: additional_ren_entry_ids_from_profile_bytes
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L282-L288
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
---

# Signature

`pub(super) fn additional_ren_entry_ids_from_profile_bytes(bytes: &[u8]) -> Option<MapiValue>`

# Calls

- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)

# Called by

- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)