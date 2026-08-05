---
type: Rust Method
title: fetch_mapi_folder_profile_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L7041-L7073
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
---

# Signature

`fn fetch_mapi_folder_profile_property_values<'a>( &'a self, account_id: Uuid, folder_id: u64, property_tags: &'a [u32], ) -> StoreFuture<'a, Vec<MapiFolderProfilePropertyValue>>`

# Called by

- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)