---
type: Rust Method
title: patch_public_folder_per_user_state
resource: crates/lpe-exchange/src/tests/mod.rs#L6739-L6783
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn patch_public_folder_per_user_state<'a>( &'a self, principal_account_id: Uuid, folder_id: Uuid, patches: &'a [PublicFolderPerUserStatePatch], ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)