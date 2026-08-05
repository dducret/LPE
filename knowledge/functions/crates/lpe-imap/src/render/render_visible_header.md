---
type: Rust Function
title: render_visible_header
resource: crates/lpe-imap/src/render.rs#L1012-L1033
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/search/searchable_header_value
---

# Signature

`pub(crate) fn render_visible_header(email: &ImapEmail) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [searchable_header_value](../../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)