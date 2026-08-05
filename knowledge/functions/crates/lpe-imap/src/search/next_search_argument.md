---
type: Rust Function
title: next_search_argument
resource: crates/lpe-imap/src/search.rs#L246-L253
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-imap/src/search/parse_search_key
---

# Signature

`fn next_search_argument(tokens: &[String], cursor: &mut usize, criterion: &str) -> Result<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_search_key](../../../../../functions/crates/lpe-imap/src/search/parse_search_key.md)