---
type: Rust Function
title: parse_partial_range
resource: crates/lpe-imap/src/render.rs#L297-L312
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/parse_body_fetch_item
---

# Signature

`fn parse_partial_range(value: &str) -> Result<Option<PartialRange>>`

# Called by

- [parse_body_fetch_item](../../../../../functions/crates/lpe-imap/src/render/parse_body_fetch_item.md)