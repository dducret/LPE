---
type: Rust Method
title: mailboxes
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L724-L729
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_only_ipm_subtree_children
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update
---

# Signature

`pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox>`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [custom_collaboration_folders_are_only_ipm_subtree_children](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_only_ipm_subtree_children.md)
- [postgres_mapi_contacts_local_commit_time_tracks_canonical_update](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update.md)