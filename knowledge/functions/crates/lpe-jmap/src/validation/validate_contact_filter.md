---
type: Rust Function
title: validate_contact_filter
resource: crates/lpe-jmap/src/validation.rs#L37-L44
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes
---

# Signature

`pub(crate) fn validate_contact_filter(filter: Option<&ContactCardQueryFilter>) -> Result<()>`

# Called by

- [handle_contact_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_contact_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)