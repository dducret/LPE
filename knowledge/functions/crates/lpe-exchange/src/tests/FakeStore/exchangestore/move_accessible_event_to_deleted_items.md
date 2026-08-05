---
type: Rust Method
title: move_accessible_event_to_deleted_items
resource: crates/lpe-exchange/src/tests/mod.rs#L8931-L9181
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn move_accessible_event_to_deleted_items<'a>( &'a self, principal_account_id: Uuid, event_id: Uuid, imported_identity: Option<MapiEventImportedMoveIdentity>, ) -> StoreFuture<'a, MoveAccessibleEventToDeletedItemsResult>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)