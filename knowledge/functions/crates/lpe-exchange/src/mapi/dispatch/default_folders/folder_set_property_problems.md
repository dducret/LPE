---
type: Rust Function
title: folder_set_property_problems
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L25-L138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_profile_bytes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_id_matches_or_is_persistable_alias_candidate
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) fn folder_set_property_problems( object: Option<&MapiObject>, mailboxes: &[JmapMailbox], values: &[(u32, MapiValue)], ) -> Vec<(usize, u32, u32)>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [additional_ren_entry_ids_profile_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_profile_bytes.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [default_folder_id_matches_or_is_persistable_alias_candidate](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_id_matches_or_is_persistable_alias_candidate.md)
- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)
- [default_folder_entry_id_expected_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)