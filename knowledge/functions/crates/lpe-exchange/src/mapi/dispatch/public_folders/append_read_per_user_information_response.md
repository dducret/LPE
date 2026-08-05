---
type: Rust Function
title: append_read_per_user_information_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L376-L415
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_folder_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_per_user_stream
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response
---

# Signature

`pub(super) async fn append_read_per_user_information_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [per_user_folder_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_folder_object_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_per_user_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_per_user_stream.md)
- [rop_read_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_per_user_information_response.md)

# Called by

- [append_public_folder_per_user_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response.md)