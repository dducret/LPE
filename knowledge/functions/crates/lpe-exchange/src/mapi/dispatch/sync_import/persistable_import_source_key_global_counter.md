---
type: Rust Function
title: persistable_import_source_key_global_counter
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L769-L772
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_source_key_identity_scope
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(super) fn persistable_import_source_key_global_counter(source_key: &[u8]) -> Option<u64>`

# Calls

- [source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)
- [import_source_key_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_source_key_identity_scope.md)

# Called by

- [imported_event_identity_from_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)