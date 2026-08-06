---
type: Rust Function
title: fake_sort_hash
resource: crates/lpe-jmap/src/tests.rs#L95-L98
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/save_jmap_query_state
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_query_state
---

# Signature

`fn fake_sort_hash(sort: Option<&Vec<Value>>) -> String`

# Called by

- [save_jmap_query_state](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_query_state.md)