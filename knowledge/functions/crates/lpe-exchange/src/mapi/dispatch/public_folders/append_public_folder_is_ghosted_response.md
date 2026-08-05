---
type: Rust Function
title: append_public_folder_is_ghosted_response
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L566-L604
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/public_folder_probe_object_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_replica_server_names
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_public_folder_is_ghosted_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_replica_probe_response
---

# Signature

`pub(super) fn append_public_folder_is_ghosted_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/logon_request_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [public_folder_probe_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/public_folder_probe_object_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_replica_server_names](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_replica_server_names.md)
- [rop_public_folder_is_ghosted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_public_folder_is_ghosted_response.md)

# Called by

- [append_public_folder_replica_probe_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_replica_probe_response.md)