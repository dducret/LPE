---
type: Rust Function
title: split_two
resource: crates/lpe-imap/src/parse.rs#L95-L101
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
  - functions/crates/lpe-imap/src/messages/parse_fetch_arguments
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
---

# Signature

`pub(crate) fn split_two(input: &str) -> Result<(&str, &str)>`

# Called by

- [handle_copy](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [parse_fetch_arguments](../../../../../functions/crates/lpe-imap/src/messages/parse_fetch_arguments.md)
- [handle_uid](../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)