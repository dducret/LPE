---
type: Rust Function
title: load_mapi_identity_scope
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L42-L82
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_identity_requests_for_mailboxes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/is_special_canonical_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test
---

# Signature

`pub(in crate::mapi) async fn load_mapi_identity_scope<S>( store: &S, account_id: Uuid, ) -> Result<MapiIdentityScope> where S: ExchangeStore,`

# Calls

- [ensure_jmap_system_mailboxes](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [context](../../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [mapi_identity_requests_for_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_identity_requests_for_mailboxes.md)
- [fetch_or_allocate_mapi_identities](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [from_special_folder_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [is_special_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/is_special_canonical_id.md)
- [remember_mapi_identity_with_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [load_mapi_identity_codec_for_test](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)