---
type: Rust Function
title: insert_non_empty_object
resource: crates/lpe-jmap/src/contacts.rs#L688-L692
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/contact_to_value
---

# Signature

`fn insert_non_empty_object(object: &mut Map<String, Value>, key: &str, value: Value)`

# Called by

- [contact_to_value](../../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)