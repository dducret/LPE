---
type: Rust Function
title: append_get_per_user_long_term_ids_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L228-L310
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_long_term_ids_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response
---

# Signature

`pub(super) async fn append_get_per_user_long_term_ids_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [public_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [rop_get_per_user_long_term_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_per_user_long_term_ids_response.md)

# Called by

- [append_public_folder_per_user_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response.md)