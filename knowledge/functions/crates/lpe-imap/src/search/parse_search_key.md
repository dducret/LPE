---
type: Rust Function
title: parse_search_key
resource: crates/lpe-imap/src/search.rs#L172-L244
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-imap/src/search/SearchExpression/from_tokens
  - functions/crates/lpe-imap/src/search/next_search_argument
  - functions/web/client/src/components/CanonicalItemEditor/Header
  - functions/crates/lpe-imap/src/search/parse_search_date
  - functions/crates/lpe-imap/src/search/looks_like_message_set
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/from_tokens
---

# Signature

`fn parse_search_key(tokens: &[String], cursor: &mut usize) -> Result<SearchExpression>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [from_tokens](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/from_tokens.md)
- [next_search_argument](../../../../../functions/crates/lpe-imap/src/search/next_search_argument.md)
- [Header](../../../../../functions/web/client/src/components/CanonicalItemEditor/Header.md)
- [parse_search_date](../../../../../functions/crates/lpe-imap/src/search/parse_search_date.md)
- [looks_like_message_set](../../../../../functions/crates/lpe-imap/src/search/looks_like_message_set.md)

# Called by

- [from_tokens](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/from_tokens.md)