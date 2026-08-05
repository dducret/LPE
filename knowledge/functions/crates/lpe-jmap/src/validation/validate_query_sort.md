---
type: Rust Function
title: validate_query_sort
resource: crates/lpe-jmap/src/validation.rs#L9-L18
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes
---

# Signature

`pub(crate) fn validate_query_sort(sort: Option<&[EmailQuerySort]>) -> Result<()>`

# Called by

- [handle_email_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_thread_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query.md)
- [handle_thread_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes.md)