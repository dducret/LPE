---
type: Rust Function
title: ensure_copy_allowed
resource: crates/lpe-imap/src/messages.rs#L485-L496
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
---

# Signature

`fn ensure_copy_allowed(source_role: &str, target_role: &str) -> Result<()>`

# Called by

- [handle_copy](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)