---
type: Rust Method
title: from_input
resource: crates/lpe-storage/src/mapi_contacts.rs#L390-L444
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/workspace/contact_emails_json
  - functions/crates/lpe-storage/src/workspace/contact_primary_email
  - functions/crates/lpe-storage/src/workspace/contact_phones_json
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/workspace/contact_array_json
  - functions/crates/lpe-storage/src/workspace/contact_source_payload_json
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`fn from_input(input: &UpsertClientContactInput) -> Result<Self>`

# Calls

- [contact_emails_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_emails_json.md)
- [contact_primary_email](../../../../../../functions/crates/lpe-storage/src/workspace/contact_primary_email.md)
- [contact_phones_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_phones_json.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [contact_array_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_array_json.md)
- [contact_source_payload_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_source_payload_json.md)

# Called by

- [create_mapi_contact](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)