---
type: Rust Function
title: trailing_literal
resource: crates/lpe-imap/src/service.rs#L168-L192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/service/read_command_literals
---

# Signature

`fn trailing_literal(line: &str) -> Result<Option<(&str, usize, bool)>>`

# Called by

- [read_command_literals](../../../../../functions/crates/lpe-imap/src/service/read_command_literals.md)