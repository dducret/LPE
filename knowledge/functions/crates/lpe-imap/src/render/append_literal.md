---
type: Rust Function
title: append_literal
resource: crates/lpe-imap/src/render.rs#L369-L372
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/append_body_section
---

# Signature

`fn append_literal(output: &mut Vec<u8>, label: &str, value: &[u8])`

# Called by

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)