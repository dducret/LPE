---
type: Rust Function
title: parse_contact_input
resource: crates/lpe-jmap/src/contacts.rs#L789-L862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/contacts/reject_unknown_contact_properties
  - functions/crates/lpe-jmap/src/contacts/validate_address_book_ids
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/contacts/parse_contact_name
  - functions/crates/lpe-jmap/src/contacts/parse_contact_title
  - functions/crates/lpe-jmap/src/contacts/parse_contact_email
  - functions/crates/lpe-jmap/src/contacts/parse_contact_phone
  - functions/crates/lpe-jmap/src/contacts/parse_contact_organization
  - functions/crates/lpe-jmap/src/contacts/parse_contact_note
  - functions/crates/lpe-jmap/src/contacts/parse_contact_name_fields
  - functions/crates/lpe-jmap/src/contacts/parse_contact_property_array
  - functions/crates/lpe-jmap/src/contacts/parse_contact_organization_name
  - functions/crates/lpe-jmap/src/contacts/parse_contact_job_title
---

# Signature

`fn parse_contact_input( id: Option<Uuid>, account_id: Uuid, value: Value, ) -> Result<(Option<String>, UpsertClientContactInput)>`

# Calls

- [reject_unknown_contact_properties](../../../../../functions/crates/lpe-jmap/src/contacts/reject_unknown_contact_properties.md)
- [validate_address_book_ids](../../../../../functions/crates/lpe-jmap/src/contacts/validate_address_book_ids.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_contact_name](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_name.md)
- [parse_contact_title](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_title.md)
- [parse_contact_email](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_email.md)
- [parse_contact_phone](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_phone.md)
- [parse_contact_organization](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization.md)
- [parse_contact_note](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_note.md)
- [parse_contact_name_fields](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_name_fields.md)
- [parse_contact_property_array](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_array.md)
- [parse_contact_organization_name](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization_name.md)
- [parse_contact_job_title](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_job_title.md)