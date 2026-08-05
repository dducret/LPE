---
type: Rust Function
title: address_book_to_value
resource: crates/lpe-jmap/src/contacts.rs#L514-L543
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get
---

# Signature

`fn address_book_to_value( collection: &CollaborationCollection, properties: &HashSet<String>, ) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_address_book_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get.md)