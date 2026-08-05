---
type: Rust Function
title: default_folder_identification_safe_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L152-L166
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_any_default_folder_identification_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_identification_values_canonicalize_additional_ren_reserved_slots
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_properties_canonicalize_additional_ren_client_prefix
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state
---

# Signature

`pub(super) fn default_folder_identification_safe_property_values( principal: &AccountPrincipal, object: Option<&MapiObject>, values: Vec<(u32, MapiValue)>, ) -> Vec<(u32, MapiValue)>`

# Calls

- [strips_any_default_folder_identification_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_any_default_folder_identification_values.md)
- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [default_folder_identification_values_canonicalize_additional_ren_reserved_slots](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_identification_values_canonicalize_additional_ren_reserved_slots.md)
- [root_default_folder_properties_canonicalize_additional_ren_client_prefix](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_properties_canonicalize_additional_ren_client_prefix.md)
- [root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state.md)