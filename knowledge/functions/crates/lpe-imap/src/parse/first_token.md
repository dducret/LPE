---
type: Rust Function
title: first_token
resource: crates/lpe-imap/src/parse.rs#L108-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path_token
---

# Signature

`pub(crate) fn first_token(arguments: &str, error: &str) -> Result<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_mailbox_path_token](../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)