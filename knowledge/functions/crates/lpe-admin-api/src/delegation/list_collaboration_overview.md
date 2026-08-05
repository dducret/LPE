---
type: Rust Function
title: list_collaboration_overview
resource: crates/lpe-admin-api/src/delegation.rs#L24-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_list_collections
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_outgoing_task_list_grants
---

# Signature

`pub(crate) async fn list_collaboration_overview( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<CollaborationOverviewResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_accessible_task_list_collections](../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_list_collections.md)
- [fetch_outgoing_collaboration_grants](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants.md)
- [fetch_outgoing_task_list_grants](../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_outgoing_task_list_grants.md)