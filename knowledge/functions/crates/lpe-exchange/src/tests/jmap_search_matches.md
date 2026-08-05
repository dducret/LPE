---
type: Rust Function
title: jmap_search_matches
resource: crates/lpe-exchange/src/tests/mod.rs#L4115-L4132
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/query_jmap_email_ids
---

# Signature

`fn jmap_search_matches(email: &JmapEmail, search_text: &str) -> bool`

# Called by

- [query_jmap_email_ids](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/query_jmap_email_ids.md)