---
type: Rust Function
title: default_folder_identification_contract_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L24-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_identification_contract_decodes_root_defaults
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_identification_contract_for_debug( principal: &AccountPrincipal, ) -> String`

# Calls

- [special_folder_identification_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [default_folder_entry_id_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)

# Called by

- [default_folder_identification_contract_decodes_root_defaults](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_identification_contract_decodes_root_defaults.md)