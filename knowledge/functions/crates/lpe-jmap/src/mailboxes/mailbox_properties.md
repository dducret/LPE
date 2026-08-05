---
type: Rust Function
title: mailbox_properties
resource: crates/lpe-jmap/src/mailboxes.rs#L472-L489
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
---

# Signature

`fn mailbox_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_mailbox_get](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)