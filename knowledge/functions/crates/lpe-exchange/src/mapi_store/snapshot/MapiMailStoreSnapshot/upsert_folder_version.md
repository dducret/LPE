---
type: Rust Method
title: upsert_folder_version
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L784-L786
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/upsert
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(crate) fn upsert_folder_version(&mut self, version: MapiFolderVersion)`

# Calls

- [upsert](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/upsert.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)