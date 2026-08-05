---
type: Rust Method
title: move_jmap_email_from_mailbox_with_mapi_identity
resource: crates/lpe-exchange/src/tests/mod.rs#L11346-L11490
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn move_jmap_email_from_mailbox_with_mapi_identity<'a>( &'a self, account_id: Uuid, source_mailbox_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, imported_identity: MapiMessageImportedMoveIdentity, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, MapiMessageMoveResult>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)