---
type: Rust Function
title: parse_contact_property_entry
resource: crates/lpe-jmap/src/contacts.rs#L1038-L1051
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_property_array
---

# Signature

`fn parse_contact_property_entry( entry: &Value, source_key: &str, target_key: &str, ) -> Result<Value>`

# Calls

- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [parse_contact_property_array](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_array.md)