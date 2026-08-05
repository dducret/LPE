---
type: Rust Function
title: parse_fetch_attributes
resource: crates/lpe-imap/src/render.rs#L90-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/parse_fetch_item_list
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
---

# Signature

`pub(crate) fn parse_fetch_attributes(input: &str) -> Result<FetchAttributes>`

# Calls

- [parse_fetch_item_list](../../../../../functions/crates/lpe-imap/src/render/parse_fetch_item_list.md)

# Called by

- [handle_fetch](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)