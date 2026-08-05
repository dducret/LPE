---
type: Rust Function
title: imported_source_global_counter
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L33-L50
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
---

# Signature

`fn imported_source_global_counter( identity: &MapiEventImportedIdentity, replica_guid: Uuid, ) -> Result<u64>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [allocate_mapi_event_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)