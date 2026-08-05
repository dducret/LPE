---
type: Rust Function
title: rop_public_folder_logon_response_body
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L44-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/public_folder_logon_response_logon_flags
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/rop/logon/gwart_time_marker
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
---

# Signature

`pub(in crate::mapi) fn rop_public_folder_logon_response_body( principal: &AccountPrincipal, request: &RopRequest, ) -> Vec<u8>`

# Calls

- [public_folder_logon_response_logon_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/public_folder_logon_response_logon_flags.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [logon_time_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [gwart_time_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/gwart_time_marker.md)

# Called by

- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)