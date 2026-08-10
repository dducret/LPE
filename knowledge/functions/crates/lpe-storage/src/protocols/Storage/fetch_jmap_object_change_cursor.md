---
type: Rust Method
title: fetch_jmap_object_change_cursor
resource: crates/lpe-storage/src/protocols.rs#L324-L349
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds
---

# Signature

`pub async fn fetch_jmap_object_change_cursor( &self, account_id: Uuid, data_type: &str, ) -> Result<Option<i64>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [jmap_object_replay_kinds](../../../../../../functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds.md)