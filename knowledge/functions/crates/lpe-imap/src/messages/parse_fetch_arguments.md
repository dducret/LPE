---
type: Rust Function
title: parse_fetch_arguments
resource: crates/lpe-imap/src/messages.rs#L412-L443
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/split_two
  - functions/crates/lpe-imap/src/messages/parse_fetch_modifier
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
---

# Signature

`fn parse_fetch_arguments(arguments: &str) -> Result<(&str, &str, Option<u64>)>`

# Calls

- [split_two](../../../../../functions/crates/lpe-imap/src/parse/split_two.md)
- [parse_fetch_modifier](../../../../../functions/crates/lpe-imap/src/messages/parse_fetch_modifier.md)

# Called by

- [handle_fetch](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)