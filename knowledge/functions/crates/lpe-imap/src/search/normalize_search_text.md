---
type: Rust Function
title: normalize_search_text
resource: crates/lpe-imap/src/search.rs#L262-L264
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
  - functions/crates/lpe-imap/src/search/searchable_sender
  - functions/crates/lpe-imap/src/search/searchable_recipients
  - functions/crates/lpe-imap/src/search/search_email_text
  - functions/crates/lpe-imap/src/search/searchable_body
  - functions/crates/lpe-imap/src/search/searchable_header_value
---

# Signature

`fn normalize_search_text(value: &str) -> String`

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)
- [searchable_sender](../../../../../functions/crates/lpe-imap/src/search/searchable_sender.md)
- [searchable_recipients](../../../../../functions/crates/lpe-imap/src/search/searchable_recipients.md)
- [search_email_text](../../../../../functions/crates/lpe-imap/src/search/search_email_text.md)
- [searchable_body](../../../../../functions/crates/lpe-imap/src/search/searchable_body.md)
- [searchable_header_value](../../../../../functions/crates/lpe-imap/src/search/searchable_header_value.md)