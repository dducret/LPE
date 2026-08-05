---
type: Rust Function
title: parse_contact_note
resource: crates/lpe-jmap/src/contacts.rs#L973-L975
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_property_string
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn parse_contact_note(value: Option<&Value>) -> Result<String>`

# Calls

- [parse_contact_property_string](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_string.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)