---
type: Rust Function
title: validate_imported_identity
resource: crates/lpe-storage/src/mapi_contacts.rs#L536-L558
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime
---

# Signature

`fn validate_imported_identity(identity: &MapiContactImportedIdentity) -> Result<()>`

# Calls

- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [normalize_filetime](../../../../../functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime.md)