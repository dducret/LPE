---
type: Rust Method
title: fetch_outgoing_task_list_grants
resource: crates/lpe-storage/src/tasks.rs#L612-L653
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares
---

# Signature

`pub async fn fetch_outgoing_task_list_grants( &self, owner_account_id: Uuid, ) -> Result<Vec<TaskListGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [list_collaboration_overview](../../../../../../functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview.md)
- [fetch_jmap_shares](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares.md)