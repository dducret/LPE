---
type: Rust Function
title: append_set_search_criteria_response
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L104-L216
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/remember_search_folder_definition
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_search_criteria_dispatch_response
---

# Signature

`pub(super) async fn append_set_search_criteria_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition.md)
- [builtin_search_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_role_for_folder_id.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [remember_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/remember_search_folder_definition.md)

# Called by

- [append_search_criteria_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_search_criteria_dispatch_response.md)