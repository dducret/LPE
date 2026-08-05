---
type: Rust Function
title: validate_imported_identity
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L15-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
---

# Signature

`pub(super) fn validate_imported_identity(identity: &MapiEventImportedIdentity) -> Result<()>`

# Calls

- [merge_predecessor_change_list](../../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)