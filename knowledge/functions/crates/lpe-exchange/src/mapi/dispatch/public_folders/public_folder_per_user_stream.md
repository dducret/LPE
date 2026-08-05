---
type: Rust Function
title: public_folder_per_user_stream
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L179-L193
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response
---

# Signature

`pub(super) fn public_folder_per_user_stream( states: &[lpe_storage::PublicFolderPerUserState], ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_read_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response.md)