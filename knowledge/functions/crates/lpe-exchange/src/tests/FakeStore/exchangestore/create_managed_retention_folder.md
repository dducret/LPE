---
type: Rust Method
title: create_managed_retention_folder
resource: crates/lpe-exchange/src/tests/mod.rs#L5183-L5242
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn create_managed_retention_folder<'a>( &'a self, input: ManagedRetentionFolderCreateInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapMailbox>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)