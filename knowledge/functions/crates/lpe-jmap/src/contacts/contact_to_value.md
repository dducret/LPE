---
type: Rust Function
title: contact_to_value
resource: crates/lpe-jmap/src/contacts.rs#L567-L673
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  - functions/crates/lpe-jmap/src/contacts/insert_non_empty_object
  - functions/crates/lpe-jmap/src/contacts/contact_array_to_named_object
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input
---

# Signature

`fn contact_to_value(contact: &AccessibleContact, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)
- [insert_non_empty_object](../../../../../functions/crates/lpe-jmap/src/contacts/insert_non_empty_object.md)
- [contact_array_to_named_object](../../../../../functions/crates/lpe-jmap/src/contacts/contact_array_to_named_object.md)

# Called by

- [handle_contact_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [contact_update_input](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input.md)