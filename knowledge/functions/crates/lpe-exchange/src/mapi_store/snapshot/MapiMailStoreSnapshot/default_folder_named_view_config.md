---
type: Rust Method
title: default_folder_named_view_config
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1338-L1352
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/properties/views/persisted_default_folder_view_entry_id
---

# Signature

`pub(crate) fn default_folder_named_view_config( &self, folder_id: u64, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [outlook_default_folder_named_view_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [persisted_default_folder_view_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/persisted_default_folder_view_entry_id.md)