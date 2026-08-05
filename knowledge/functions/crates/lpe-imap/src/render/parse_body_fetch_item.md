---
type: Rust Function
title: parse_body_fetch_item
resource: crates/lpe-imap/src/render.rs#L274-L295
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/parse_partial_range
  called_by:
  - functions/crates/lpe-imap/src/render/parse_fetch_item
---

# Signature

`fn parse_body_fetch_item(raw: &str) -> Result<FetchItem>`

# Calls

- [parse_partial_range](../../../../../functions/crates/lpe-imap/src/render/parse_partial_range.md)

# Called by

- [parse_fetch_item](../../../../../functions/crates/lpe-imap/src/render/parse_fetch_item.md)