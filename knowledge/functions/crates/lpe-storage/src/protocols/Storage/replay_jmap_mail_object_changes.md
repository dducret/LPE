---
type: Rust Method
title: replay_jmap_mail_object_changes
resource: crates/lpe-storage/src/protocols.rs#L185-L321
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/protocols/is_mapi_only_change
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/protocols/jmap_change_kind
---

# Signature

`pub async fn replay_jmap_mail_object_changes( &self, account_id: Uuid, data_type: &str, after_cursor: i64, max_rows: u64, ) -> Result<Option<Vec<JmapMailObjectChange>>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [is_mapi_only_change](../../../../../../functions/crates/lpe-storage/src/protocols/is_mapi_only_change.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [jmap_change_kind](../../../../../../functions/crates/lpe-storage/src/protocols/jmap_change_kind.md)