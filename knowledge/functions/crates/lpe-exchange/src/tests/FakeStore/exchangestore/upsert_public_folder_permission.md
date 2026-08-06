---
type: Rust Method
title: upsert_public_folder_permission
resource: crates/lpe-exchange/src/tests/mod.rs#L6529-L6605
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn upsert_public_folder_permission<'a>( &'a self, input: PublicFolderPermissionInput, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, PublicFolderPermission>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)