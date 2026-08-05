---
type: Rust Method
title: change_number
resource: crates/lpe-exchange/src/mapi_store/folder_versions.rs#L49-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/version
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number
---

# Signature

`pub(super) fn change_number(&self, folder_id: u64) -> Option<u64>`

# Calls

- [version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/version.md)

# Called by

- [folder_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number.md)