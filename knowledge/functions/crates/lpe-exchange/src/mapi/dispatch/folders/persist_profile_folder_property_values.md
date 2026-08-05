---
type: Rust Function
title: persist_profile_folder_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1198-L1256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_profile_bytes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_folder_profile_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) async fn persist_profile_folder_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, values: &[(u32, MapiValue)], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [additional_ren_entry_ids_profile_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_profile_bytes.md)
- [upsert_mapi_folder_profile_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_folder_profile_property_values.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)