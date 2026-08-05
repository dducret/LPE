---
type: Rust Function
title: parse_fetch_item_list
resource: crates/lpe-imap/src/render.rs#L176-L211
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/strip_wrapping_parens
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/render/parse_fetch_item
  called_by:
  - functions/crates/lpe-imap/src/render/parse_fetch_attributes
---

# Signature

`fn parse_fetch_item_list(input: &str) -> Result<Vec<FetchItem>>`

# Calls

- [strip_wrapping_parens](../../../../../functions/crates/lpe-imap/src/render/strip_wrapping_parens.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_fetch_item](../../../../../functions/crates/lpe-imap/src/render/parse_fetch_item.md)

# Called by

- [parse_fetch_attributes](../../../../../functions/crates/lpe-imap/src/render/parse_fetch_attributes.md)