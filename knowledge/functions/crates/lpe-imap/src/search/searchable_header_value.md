---
type: Rust Function
title: searchable_header_value
resource: crates/lpe-imap/src/search.rs#L295-L308
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/search/searchable_sender
  - functions/crates/lpe-imap/src/search/searchable_recipients
  - functions/crates/lpe-imap/src/search/normalize_search_text
  - functions/crates/lpe-imap/src/render/render_visible_header
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
---

# Signature

`fn searchable_header_value(email: &ImapEmail, name: &str) -> String`

# Calls

- [searchable_sender](../../../../../functions/crates/lpe-imap/src/search/searchable_sender.md)
- [searchable_recipients](../../../../../functions/crates/lpe-imap/src/search/searchable_recipients.md)
- [normalize_search_text](../../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)
- [render_visible_header](../../../../../functions/crates/lpe-imap/src/render/render_visible_header.md)

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)