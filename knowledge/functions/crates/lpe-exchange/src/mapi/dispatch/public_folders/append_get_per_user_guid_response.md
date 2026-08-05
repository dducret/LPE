---
type: Rust Function
title: append_get_per_user_guid_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L312-L374
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_guid_response
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response
---

# Signature

`pub(super) async fn append_get_per_user_guid_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [long_term_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/long_term_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [fetch_mapi_identities_by_object_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)
- [rop_get_per_user_guid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_guid_response.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)

# Called by

- [append_public_folder_per_user_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response.md)