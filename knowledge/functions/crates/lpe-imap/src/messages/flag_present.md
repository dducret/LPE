---
type: Rust Function
title: flag_present
resource: crates/lpe-imap/src/messages.rs#L526-L528
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`fn flag_present(flags: &std::collections::HashSet<String>, expected: &str) -> bool`

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)