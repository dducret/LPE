---
type: Rust Method
title: folders
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1548-L1550
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_user_saved_search_folder_as_mapi_folder
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_deduplicates_user_saved_search_folder_projection_by_name
---

# Signature

`pub(crate) fn folders(&self) -> &[MapiFolder]`

# Called by

- [snapshot_projects_user_saved_search_folder_as_mapi_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_user_saved_search_folder_as_mapi_folder.md)
- [snapshot_deduplicates_user_saved_search_folder_projection_by_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_deduplicates_user_saved_search_folder_projection_by_name.md)