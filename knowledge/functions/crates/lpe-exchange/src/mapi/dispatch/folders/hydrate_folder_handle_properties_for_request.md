---
type: Rust Function
title: hydrate_folder_handle_properties_for_request
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1058-L1187
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/optional_folder_profile_read
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_profile_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(super) async fn hydrate_folder_handle_properties_for_request<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, property_tags: &[u32], ) where S: ExchangeStore,`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [search_folder_definition_was_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted.md)
- [search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition.md)
- [search_folder_handle_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties.md)
- [optional_folder_profile_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/optional_folder_profile_read.md)
- [fetch_mapi_folder_profile_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_profile_property_values.md)
- [additional_ren_entry_ids_from_profile_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes.md)
- [merge_additional_ren_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response.md)
- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)