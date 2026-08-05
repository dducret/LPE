---
type: Rust Function
title: logon_property_value
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L25-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  - functions/crates/lpe-exchange/src/mapi/properties/folder/valid_folder_mask
  - functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/logon_projects_outlook_bootstrap_identity_metadata
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_outlook_logon_bootstrap_property_details
  - functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug
  - functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row
---

# Signature

`pub(in crate::mapi) fn logon_property_value( principal: &AccountPrincipal, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [serialized_replid_guid_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)
- [valid_folder_mask](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/valid_folder_mask.md)
- [mailbox_owner_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)

# Called by

- [logon_projects_outlook_bootstrap_identity_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/logon_projects_outlook_bootstrap_identity_metadata.md)
- [property_is_unsupported_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [outlook_logon_bootstrap_row_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape.md)
- [format_outlook_logon_bootstrap_property_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_outlook_logon_bootstrap_property_details.md)
- [semantic_property_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)
- [write_logon_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row.md)
- [serialize_logon_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row.md)