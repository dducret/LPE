---
type: Rust Function
title: ensure_store_flags_supported
resource: crates/lpe-imap/src/messages.rs#L514-L524
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`fn ensure_store_flags_supported(flags: &std::collections::HashSet<String>) -> Result<()>`

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)