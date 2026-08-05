---
type: Rust Function
title: append_write_per_user_information_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L417-L477
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_folder_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_data_offset
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_has_finished
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_per_user_patches
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_write_data
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_per_user_information_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response
---

# Signature

`pub(super) async fn append_write_per_user_information_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [per_user_folder_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_folder_object_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [per_user_data_offset](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_data_offset.md)
- [per_user_has_finished](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_has_finished.md)
- [public_folder_per_user_patches](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_per_user_patches.md)
- [per_user_write_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/per_user_write_data.md)
- [rop_write_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_per_user_information_response.md)

# Called by

- [append_public_folder_per_user_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_per_user_response.md)