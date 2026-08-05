---
type: Rust Function
title: append_logon_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L98-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/log_default_folder_discovery_contract
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_dispatch_response
---

# Signature

`pub(super) fn append_logon_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, typed_request: &TypedRopRequest, principal: &AccountPrincipal, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [log_rop_logon_request_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_rop_logon_request_identity.md)
- [allocate_logon_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context.md)
- [identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)
- [rop_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)
- [log_default_folder_discovery_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/log_default_folder_discovery_contract.md)
- [rop_public_folder_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body.md)
- [log_outlook_bootstrap_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_logon_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_dispatch_response.md)