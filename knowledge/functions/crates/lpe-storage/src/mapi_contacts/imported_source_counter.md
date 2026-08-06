---
type: Rust Function
title: imported_source_counter
resource: crates/lpe-storage/src/mapi_contacts.rs#L1329-L1348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
---

# Signature

`fn imported_source_counter( identity: &MapiContactImportedIdentity, replica_guid: Uuid, ) -> Result<u64>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)