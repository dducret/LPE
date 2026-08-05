---
type: Rust Function
title: serialize_email_query_sort
resource: crates/lpe-jmap/src/mail/values.rs#L24-L29
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

`pub(crate) fn serialize_email_query_sort(sort: &[EmailQuerySort]) -> Result<Vec<Value>>`

# Called by

- [handle_email_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_thread_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query.md)
- [handle_thread_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes.md)