---
type: Rust Function
title: fake_filter_hash
resource: crates/lpe-jmap/src/tests.rs#L90-L92
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/save_jmap_query_state
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_query_state
---

# Signature

`fn fake_filter_hash(filter: Option<&Value>) -> String`

# Called by

- [save_jmap_query_state](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_query_state.md)