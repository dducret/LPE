---
type: Rust Function
title: contact_emails_json
resource: crates/lpe-storage/src/workspace.rs#L958-L967
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/workspace/contact_array_json
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`pub(crate) fn contact_emails_json(input: &UpsertClientContactInput) -> Result<Value>`

# Calls

- [contact_array_json](../../../../../functions/crates/lpe-storage/src/workspace/contact_array_json.md)

# Called by

- [from_input](../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)