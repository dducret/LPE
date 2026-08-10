---
type: Rust Method
title: save_jmap_query_state
resource: crates/lpe-storage/src/jmap_queries.rs#L34-L67
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/jmap_queries/jmap_query_hash
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`pub async fn save_jmap_query_state( &self, account_id: Uuid, method_name: &str, filter: Option<Value>, sort: Option<Vec<Value>>, last_change_sequence: i64, snapshot_ids: &[String], ) -> Result<Uuid>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [jmap_query_hash](../../../../../../functions/crates/lpe-storage/src/jmap_queries/jmap_query_hash.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)