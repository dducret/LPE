---
type: Rust Function
title: parse_flag_list
resource: crates/lpe-imap/src/store_args.rs#L94-L105
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`pub(crate) fn parse_flag_list(token: &str) -> Result<HashSet<String>>`

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)