---
type: Rust Function
title: address_book_properties
resource: crates/lpe-jmap/src/contacts.rs#L500-L513
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get
---

# Signature

`fn address_book_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_address_book_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get.md)