---
type: Rust Function
title: parse_contact_property_string
resource: crates/lpe-jmap/src/contacts.rs#L985-L1012
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_email
  - functions/crates/lpe-jmap/src/contacts/parse_contact_phone
  - functions/crates/lpe-jmap/src/contacts/parse_contact_organization
  - functions/crates/lpe-jmap/src/contacts/parse_contact_title
  - functions/crates/lpe-jmap/src/contacts/parse_contact_note
  - functions/crates/lpe-jmap/src/contacts/parse_contact_organization_name
  - functions/crates/lpe-jmap/src/contacts/parse_contact_job_title
---

# Signature

`fn parse_contact_property_string( value: Option<&Value>, property_name: &str, field_name: &str, ) -> Result<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_contact_email](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_email.md)
- [parse_contact_phone](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_phone.md)
- [parse_contact_organization](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization.md)
- [parse_contact_title](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_title.md)
- [parse_contact_note](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_note.md)
- [parse_contact_organization_name](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization_name.md)
- [parse_contact_job_title](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_job_title.md)