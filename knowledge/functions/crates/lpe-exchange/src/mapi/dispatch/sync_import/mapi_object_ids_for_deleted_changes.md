---
type: Rust Function
title: mapi_object_ids_for_deleted_changes
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1138-L1185
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_message_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder
---

# Signature

`pub(super) async fn mapi_object_ids_for_deleted_changes<S>( store: &S, principal: &AccountPrincipal, object_kind: MapiIdentityObjectKind, object_ids: &[Uuid], ) -> Result<Vec<u64>> where S: ExchangeStore,`

# Calls

- [fetch_mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [mapi_message_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_message_ids_for_deleted_changes.md)
- [deleted_special_object_ids_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder.md)