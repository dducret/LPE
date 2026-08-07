---
type: Rust Function
title: contact_primary_email
resource: crates/lpe-storage/src/workspace.rs#L1160-L1172
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`pub(crate) fn contact_primary_email(value: &Value) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [from_input](../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [upsert_client_contact_in_book_role](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)