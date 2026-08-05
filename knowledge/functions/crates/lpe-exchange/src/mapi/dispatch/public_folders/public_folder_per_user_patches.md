---
type: Rust Function
title: public_folder_per_user_patches
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L195-L226
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
---

# Signature

`pub(super) fn public_folder_per_user_patches( data: &[u8], ) -> Option<Vec<lpe_storage::PublicFolderPerUserStatePatch>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_write_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)