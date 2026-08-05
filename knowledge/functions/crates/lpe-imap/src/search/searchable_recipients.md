---
type: Rust Function
title: searchable_recipients
resource: crates/lpe-imap/src/search.rs#L273-L275
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/search/normalize_search_text
  - functions/crates/lpe-imap/src/render/render_recipient_header
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
  - functions/crates/lpe-imap/src/search/searchable_header_value
---

# Signature

`fn searchable_recipients(recipients: &[JmapEmailAddress]) -> String`

# Calls

- [normalize_search_text](../../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)
- [render_recipient_header](../../../../../functions/crates/lpe-imap/src/render/render_recipient_header.md)

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)
- [searchable_header_value](../../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)