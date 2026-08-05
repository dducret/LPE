---
type: Rust Function
title: reject_unknown_contact_properties
resource: crates/lpe-jmap/src/contacts.rs#L864-L874
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn reject_unknown_contact_properties(object: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)