---
type: Rust Function
title: imported_event_identity_from_properties
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L23-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn imported_event_identity_from_properties( properties: &HashMap<u32, MapiValue>, ) -> Result<Option<MapiEventImportedIdentity>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [persistable_import_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)