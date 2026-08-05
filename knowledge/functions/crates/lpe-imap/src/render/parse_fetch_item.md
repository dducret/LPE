---
type: Rust Function
title: parse_fetch_item
resource: crates/lpe-imap/src/render.rs#L235-L272
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/parse_body_fetch_item
  called_by:
  - functions/crates/lpe-imap/src/render/parse_fetch_item_list
---

# Signature

`fn parse_fetch_item(raw: &str) -> Result<FetchItem>`

# Calls

- [parse_body_fetch_item](../../../../../functions/crates/lpe-imap/src/render/parse_body_fetch_item.md)

# Called by

- [parse_fetch_item_list](../../../../../functions/crates/lpe-imap/src/render/parse_fetch_item_list.md)