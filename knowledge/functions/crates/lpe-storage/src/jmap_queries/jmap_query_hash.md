---
type: Rust Function
title: jmap_query_hash
resource: crates/lpe-storage/src/jmap_queries.rs#L335-L339
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/jmap_queries/Storage/save_jmap_query_state
  - functions/crates/lpe-storage/src/jmap_queries/Storage/fetch_jmap_query_state
---

# Signature

`fn jmap_query_hash<T: Serialize>(value: Option<&T>) -> Result<String>`

# Called by

- [save_jmap_query_state](../../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/fetch_jmap_query_state.md)