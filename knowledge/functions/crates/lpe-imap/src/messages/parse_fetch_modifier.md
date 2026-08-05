---
type: Rust Function
title: parse_fetch_modifier
resource: crates/lpe-imap/src/messages.rs#L445-L458
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/parse_fetch_arguments
---

# Signature

`fn parse_fetch_modifier(modifier: &str) -> Result<Option<u64>>`

# Called by

- [parse_fetch_arguments](../../../../../functions/crates/lpe-imap/src/messages/parse_fetch_arguments.md)