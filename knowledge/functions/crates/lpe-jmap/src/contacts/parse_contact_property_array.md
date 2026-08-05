---
type: Rust Function
title: parse_contact_property_array
resource: crates/lpe-jmap/src/contacts.rs#L1014-L1036
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/contacts/parse_contact_property_entry
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn parse_contact_property_array( value: Option<&Value>, source_key: &str, target_key: &str, ) -> Result<Value>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_contact_property_entry](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_entry.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)