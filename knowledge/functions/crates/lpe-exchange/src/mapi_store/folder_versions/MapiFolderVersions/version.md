---
type: Rust Method
title: version
resource: crates/lpe-exchange/src/mapi_store/folder_versions.rs#L37-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/change_number
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
---

# Signature

`pub(super) fn version(&self, folder_id: u64) -> Option<&MapiFolderVersion>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/change_number.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)