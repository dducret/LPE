---
type: Rust Module
title: jmap_queries
resource: crates/lpe-storage/src/jmap_queries.rs#L1-L339
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/serde-serialize
  - external/serde-json-value
  - external/sha2-digest-sha256
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [JmapEmailQuery](../../../../classes/crates/lpe-storage/src/jmap_queries/JmapEmailQuery.md)
- [JmapThreadQuery](../../../../classes/crates/lpe-storage/src/jmap_queries/JmapThreadQuery.md)
- [JmapStoredQueryState](../../../../classes/crates/lpe-storage/src/jmap_queries/JmapStoredQueryState.md)
- [save_jmap_query_state](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/fetch_jmap_query_state.md)
- [query_jmap_email_ids](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/query_jmap_email_ids.md)
- [fetch_all_jmap_email_ids](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/fetch_all_jmap_email_ids.md)
- [fetch_all_jmap_thread_ids](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/fetch_all_jmap_thread_ids.md)
- [query_jmap_thread_ids](../../../../functions/crates/lpe-storage/src/jmap_queries/Storage/query_jmap_thread_ids.md)
- [jmap_query_hash](../../../../functions/crates/lpe-storage/src/jmap_queries/jmap_query_hash.md)

# Imports

- `anyhow::Result`
- `serde::Serialize`
- `serde_json::Value`
- `sha2::{Digest, Sha256}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::Storage`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)