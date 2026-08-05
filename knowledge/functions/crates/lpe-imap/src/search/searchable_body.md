---
type: Rust Function
title: searchable_body
resource: crates/lpe-imap/src/search.rs#L287-L293
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/search/normalize_search_text
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
---

# Signature

`fn searchable_body(email: &ImapEmail) -> String`

# Calls

- [normalize_search_text](../../../../../functions/crates/lpe-imap/src/search/normalize_search_text.md)

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)