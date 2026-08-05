---
type: Rust Function
title: parse_store_mode
resource: crates/lpe-imap/src/store_args.rs#L64-L92
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`pub(crate) fn parse_store_mode(token: &str) -> Result<StoreMode>`

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)