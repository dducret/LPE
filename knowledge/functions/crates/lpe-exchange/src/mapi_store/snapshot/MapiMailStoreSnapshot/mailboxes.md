---
type: Rust Method
title: mailboxes
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L765-L770
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update
---

# Signature

`pub(crate) fn mailboxes(&self) -> Vec<JmapMailbox>`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection.md)
- [custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants.md)
- [postgres_mapi_contacts_local_commit_time_tracks_canonical_update](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_contacts_local_commit_time_tracks_canonical_update.md)