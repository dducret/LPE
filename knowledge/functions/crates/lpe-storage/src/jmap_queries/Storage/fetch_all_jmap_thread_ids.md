---
type: Rust Method
title: fetch_all_jmap_thread_ids
resource: crates/lpe-storage/src/jmap_queries.rs#L224-L247
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)