---
type: Rust Function
title: source_key_global_counter
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L774-L781
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_property_source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
---

# Signature

`pub(super) fn source_key_global_counter(source_key: &[u8]) -> Option<u64>`

# Calls

- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [imported_property_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_property_source_key_global_counter.md)
- [persistable_import_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)