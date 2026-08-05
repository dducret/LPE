---
type: Rust Function
title: email_properties
resource: crates/lpe-jmap/src/mail/values.rs#L142-L169
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get
---

# Signature

`pub(crate) fn email_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_email_get](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get.md)