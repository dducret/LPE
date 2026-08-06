---
type: Rust Function
title: normalize_filetime
resource: crates/lpe-storage/src/mapi_contacts.rs#L560-L565
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/validate_imported_identity
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
---

# Signature

`fn normalize_filetime(value: u64) -> Result<u64>`

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [validate_imported_identity](../../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_imported_identity.md)
- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)