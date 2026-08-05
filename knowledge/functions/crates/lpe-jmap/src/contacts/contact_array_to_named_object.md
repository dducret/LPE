---
type: Rust Function
title: contact_array_to_named_object
resource: crates/lpe-jmap/src/contacts.rs#L694-L726
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-jmap/src/contacts/contact_to_value
---

# Signature

`fn contact_array_to_named_object(value: &Value, source_key: &str, target_key: &str) -> Value`

# Calls

- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [contact_to_value](../../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)