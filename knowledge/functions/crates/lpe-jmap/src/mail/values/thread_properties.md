---
type: Rust Function
title: thread_properties
resource: crates/lpe-jmap/src/mail/values.rs#L210-L215
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get
---

# Signature

`pub(crate) fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_thread_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)