---
type: Rust Method
title: fetch_accessible_task_list_collections
resource: crates/lpe-storage/src/collaboration.rs#L499-L509
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview
---

# Signature

`pub async fn fetch_accessible_task_list_collections( &self, principal_account_id: Uuid, ) -> Result<Vec<CollaborationCollection>>`

# Called by

- [list_collaboration_overview](../../../../../../functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview.md)