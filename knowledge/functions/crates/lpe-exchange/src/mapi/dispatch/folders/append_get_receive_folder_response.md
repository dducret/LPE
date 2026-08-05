---
type: Rust Function
title: append_get_receive_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L111-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/valid_receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/explicit_receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_get_receive_folder_contract
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_receive_folder_dispatch_response
---

# Signature

`pub(super) fn append_get_receive_folder_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [private_logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/receive_folder_message_class.md)
- [valid_receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/valid_receive_folder_message_class.md)
- [receive_folder_id_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class.md)
- [rop_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_response.md)
- [explicit_receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/explicit_receive_folder_message_class.md)
- [record_receive_folder_verification_passed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [record_post_hierarchy_request_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [post_hierarchy_get_receive_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_get_receive_folder_contract.md)

# Called by

- [append_receive_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_receive_folder_dispatch_response.md)