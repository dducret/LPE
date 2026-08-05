---
type: Rust Function
title: searchable_sender
resource: crates/lpe-imap/src/search.rs#L266-L271
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/search/normalize_search_text
  - functions/crates/lpe-imap/src/render/render_address_header
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
  - functions/crates/lpe-imap/src/search/searchable_header_value
---

# Signature

`fn searchable_sender(email: &ImapEmail) -> String`

# Calls

- [normalize_search_text](../../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)
- [render_address_header](../../../../../functions/crates/lpe-imap/src/render/render_address_header.md)

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)
- [searchable_header_value](../../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)