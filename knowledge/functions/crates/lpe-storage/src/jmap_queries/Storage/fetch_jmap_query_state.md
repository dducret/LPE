---
type: Rust Method
title: fetch_jmap_query_state
resource: crates/lpe-storage/src/jmap_queries.rs#L69-L114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/jmap_queries/jmap_query_hash
---

# Signature

`pub async fn fetch_jmap_query_state( &self, account_id: Uuid, method_name: &str, state_id: Uuid, filter: Option<Value>, sort: Option<Vec<Value>>, ) -> Result<Option<JmapStoredQueryState>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [jmap_query_hash](../../../../../../functions/crates/lpe-storage/src/jmap_queries/jmap_query_hash.md)