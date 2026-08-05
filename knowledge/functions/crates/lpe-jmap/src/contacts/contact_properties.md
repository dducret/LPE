---
type: Rust Function
title: contact_properties
resource: crates/lpe-jmap/src/contacts.rs#L545-L565
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input
---

# Signature

`fn contact_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_contact_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [contact_update_input](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input.md)