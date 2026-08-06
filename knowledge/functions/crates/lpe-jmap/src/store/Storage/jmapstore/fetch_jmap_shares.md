---
type: Rust Method
title: fetch_jmap_shares
resource: crates/lpe-jmap/src/store.rs#L1256-L1287
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/store/shares/project_share
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_outgoing_task_list_grants
---

# Signature

`async fn fetch_jmap_shares(&self, account_id: Uuid) -> Result<Vec<Value>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [project_share](../../../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)
- [fetch_outgoing_collaboration_grants](../../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants.md)
- [fetch_outgoing_task_list_grants](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_outgoing_task_list_grants.md)