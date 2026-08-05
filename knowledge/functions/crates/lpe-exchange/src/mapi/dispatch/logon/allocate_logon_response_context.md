---
type: Rust Function
title: allocate_logon_response_context
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L55-L96
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logon_identity
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/replica_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
---

# Signature

`pub(super) fn allocate_logon_response_context( session: &mut MapiSession, handle_slots: &mut Vec<u32>, principal: &AccountPrincipal, request: &RopRequest, identity_codec: &crate::mapi::identity::MapiIdentityCodec, ) -> LogonResponseContext`

# Calls

- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [record_logon_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logon_identity.md)
- [replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/replica_guid.md)

# Called by

- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)