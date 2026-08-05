---
type: Rust Function
title: contact_object_string
resource: crates/lpe-jmap/src/contacts.rs#L935-L942
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_name_fields
---

# Signature

`fn contact_object_string(object: &Map<String, Value>, key: &str) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_contact_name_fields](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_name_fields.md)