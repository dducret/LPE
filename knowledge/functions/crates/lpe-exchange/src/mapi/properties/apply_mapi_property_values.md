---
type: Rust Function
title: apply_mapi_property_values
resource: crates/lpe-exchange/src/mapi/properties.rs#L1329-L1384
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/ipm_subtree_ostid_write_is_retained_as_session_mutable_state
  - functions/crates/lpe-exchange/src/mapi/rop/tests/ipm_subtree_ostid_read_prefers_session_client_write
---

# Signature

`pub(in crate::mapi) fn apply_mapi_property_values( object: Option<&mut MapiObject>, values: Vec<(u32, MapiValue)>, ) -> Result<()>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [apply_pending_associated_message_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values.md)
- [is_default_folder_identification_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)

# Called by

- [append_set_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state.md)
- [ipm_subtree_ostid_write_is_retained_as_session_mutable_state](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/ipm_subtree_ostid_write_is_retained_as_session_mutable_state.md)
- [ipm_subtree_ostid_read_prefers_session_client_write](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/ipm_subtree_ostid_read_prefers_session_client_write.md)