---
type: Rust Function
title: parse_contact_email
resource: crates/lpe-jmap/src/contacts.rs#L944-L947
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_property_string
  - functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn parse_contact_email(value: Option<&Value>) -> Result<String>`

# Calls

- [parse_contact_property_string](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_string.md)
- [normalize_trimmed_lowercase](../../../../../functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)