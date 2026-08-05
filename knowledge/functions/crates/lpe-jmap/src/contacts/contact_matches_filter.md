---
type: Rust Function
title: contact_matches_filter
resource: crates/lpe-jmap/src/contacts.rs#L728-L759
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes
---

# Signature

`fn contact_matches_filter(contact: &AccessibleContact, filter: &ContactCardQueryFilter) -> bool`

# Called by

- [handle_contact_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_contact_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)