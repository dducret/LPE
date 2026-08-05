---
type: Rust Function
title: parse_contact_name_fields
resource: crates/lpe-jmap/src/contacts.rs#L908-L933
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/contacts/contact_object_string
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn parse_contact_name_fields(value: Option<&Value>) -> Result<ContactNameFields>`

# Calls

- [contact_object_string](../../../../../functions/crates/lpe-jmap/src/contacts/contact_object_string.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)