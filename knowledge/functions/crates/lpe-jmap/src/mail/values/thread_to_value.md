---
type: Rust Function
title: thread_to_value
resource: crates/lpe-jmap/src/mail/values.rs#L584-L598
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get
---

# Signature

`pub(crate) fn thread_to_value( thread_id: Uuid, email_ids: Vec<String>, properties: &HashSet<String>, ) -> Value`

# Calls

- [insert_if](../../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_thread_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)