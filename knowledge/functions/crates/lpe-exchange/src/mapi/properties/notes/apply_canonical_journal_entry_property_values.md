---
type: Rust Function
title: apply_canonical_journal_entry_property_values
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L458-L482
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/reject_unsupported_mapi_journal_entry_properties
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_journal_entry_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, journal_entry_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [reject_unsupported_mapi_journal_entry_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/reject_unsupported_mapi_journal_entry_properties.md)
- [journal_entry_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)