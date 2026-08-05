---
type: Rust Function
title: remember_created_mapi_identity_record
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1356-L1386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_message_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(super) async fn remember_created_mapi_identity_record<S>( store: &S, principal: &AccountPrincipal, object_kind: MapiIdentityObjectKind, canonical_id: Uuid, reserved_global_counter: Option<u64>, source_key: Option<Vec<u8>>, ) -> Result<crate::store::MapiIdentityRecord> where S: ExchangeStore,`

# Calls

- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)

# Called by

- [remember_created_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [remember_created_message_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_message_mapi_identity.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)