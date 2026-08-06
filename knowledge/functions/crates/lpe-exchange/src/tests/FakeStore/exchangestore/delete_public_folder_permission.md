---
type: Rust Method
title: delete_public_folder_permission
resource: crates/lpe-exchange/src/tests/mod.rs#L6607-L6630
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn delete_public_folder_permission<'a>( &'a self, _principal_account_id: Uuid, folder_id: Uuid, grantee_account_id: Uuid, audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)