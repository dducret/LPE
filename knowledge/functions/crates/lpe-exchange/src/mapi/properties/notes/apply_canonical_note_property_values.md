---
type: Rust Function
title: apply_canonical_note_property_values
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L432-L456
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/reject_unsupported_mapi_note_properties
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_note_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, note_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [reject_unsupported_mapi_note_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/reject_unsupported_mapi_note_properties.md)
- [note_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)