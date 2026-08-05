---
type: Rust Function
title: strip_search_return_options
resource: crates/lpe-imap/src/messages.rs#L471-L483
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_search
---

# Signature

`fn strip_search_return_options(tokens: &mut Vec<String>) -> Result<()>`

# Called by

- [handle_search](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_search.md)