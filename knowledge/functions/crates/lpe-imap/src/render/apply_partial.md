---
type: Rust Function
title: apply_partial
resource: crates/lpe-imap/src/render.rs#L360-L367
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/append_body_section
---

# Signature

`fn apply_partial(value: &[u8], partial: Option<PartialRange>) -> (Option<usize>, &[u8])`

# Called by

- [append_body_section](../../../../../functions/crates/lpe-imap/src/render/append_body_section.md)