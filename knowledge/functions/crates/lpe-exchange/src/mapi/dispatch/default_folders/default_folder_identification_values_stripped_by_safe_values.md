---
type: Rust Function
title: default_folder_identification_values_stripped_by_safe_values
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L372-L379
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug
---

# Signature

`pub(super) fn default_folder_identification_values_stripped_by_safe_values( object: Option<&MapiObject>, property_tags: &[u32], ) -> bool`

# Calls

- [strips_default_folder_identification_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value.md)

# Called by

- [post_hierarchy_setprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract.md)
- [log_set_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug.md)