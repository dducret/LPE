---
type: Rust Function
title: parse_contact_name
resource: crates/lpe-jmap/src/contacts.rs#L893-L906
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn parse_contact_name(value: Option<&Value>) -> Result<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)