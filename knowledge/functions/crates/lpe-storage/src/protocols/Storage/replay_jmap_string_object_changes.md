---
type: Rust Method
title: replay_jmap_string_object_changes
resource: crates/lpe-storage/src/protocols.rs#L452-L533
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/protocols/jmap_change_kind
---

# Signature

`pub async fn replay_jmap_string_object_changes( &self, account_id: Uuid, data_type: &str, after_cursor: i64, max_rows: u64, ) -> Result<Option<Vec<JmapStringObjectChange>>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [jmap_object_replay_kinds](../../../../../../functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [jmap_string_replay_object_id](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [jmap_change_kind](../../../../../../functions/crates/lpe-storage/src/protocols/jmap_change_kind.md)