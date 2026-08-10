---
type: Rust Function
title: contact_array_json
resource: crates/lpe-storage/src/workspace.rs#L1012-L1018
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
  - functions/crates/lpe-storage/src/workspace/contact_emails_json
  - functions/crates/lpe-storage/src/workspace/contact_phones_json
---

# Signature

`pub(crate) fn contact_array_json(value: Option<Value>) -> Result<Value>`

# Called by

- [from_input](../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [contact_emails_json](../../../../../functions/crates/lpe-storage/src/workspace/contact_emails_json.md)
- [contact_phones_json](../../../../../functions/crates/lpe-storage/src/workspace/contact_phones_json.md)