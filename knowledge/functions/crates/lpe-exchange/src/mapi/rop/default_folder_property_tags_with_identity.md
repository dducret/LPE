---
type: Rust Function
title: default_folder_property_tags_with_identity
resource: crates/lpe-exchange/src/mapi/rop.rs#L1158-L1162
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_identity_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response
---

# Signature

`fn default_folder_property_tags_with_identity() -> Vec<u32>`

# Calls

- [default_folder_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)
- [default_folder_identity_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_identity_property_tags.md)

# Called by

- [rop_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [rop_get_properties_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response.md)