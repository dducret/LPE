---
type: Rust Function
title: append_public_folder_per_user_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L479-L527
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_metadata_dispatch_response
---

# Signature

`pub(super) async fn append_public_folder_per_user_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [append_get_per_user_long_term_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_long_term_ids_response.md)
- [append_get_per_user_guid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response.md)
- [append_read_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_read_per_user_information_response.md)
- [append_write_per_user_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_write_per_user_information_response.md)

# Called by

- [append_public_folder_metadata_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_metadata_dispatch_response.md)