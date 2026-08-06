---
type: Rust Function
title: validate_mapi_contact_commit_input
resource: crates/lpe-storage/src/mapi_contacts.rs#L755-L779
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/mapi_contacts/validate_custom_properties
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
---

# Signature

`fn validate_mapi_contact_commit_input(input: &MapiContactCommitInput) -> Result<()>`

# Calls

- [from_input](../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [validate_custom_properties](../../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_custom_properties.md)

# Called by

- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)