---
type: Rust Method
title: with_public_folders
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L600-L645
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_public_folder_contract
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_public_folders( mut self, folders: Vec<PublicFolder>, items: Vec<PublicFolderItem>, permissions: Vec<PublicFolderPermission>, ) -> Self`

# Calls

- [mapi_public_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_public_folder_id.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [folder_properties_for_open_projects_public_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_public_folder_contract.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)