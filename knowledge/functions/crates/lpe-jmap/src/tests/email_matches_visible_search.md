---
type: Rust Function
title: email_matches_visible_search
resource: crates/lpe-jmap/src/tests.rs#L206-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/query_jmap_email_ids
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/query_jmap_thread_ids
---

# Signature

`fn email_matches_visible_search(email: &JmapEmail, search_text: Option<&str>) -> bool`

# Called by

- [query_jmap_email_ids](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/query_jmap_email_ids.md)
- [query_jmap_thread_ids](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/query_jmap_thread_ids.md)