---
type: Rust Function
title: rop_logon_response_body
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L18-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/private_logon_response_logon_flags
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/rop/logon/gwart_time_marker
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields
  - functions/crates/lpe-exchange/src/mapi/rop/tests/private_logon_places_exactly_13_folder_ids_before_response_flags
  - functions/crates/lpe-exchange/src/mapi/rop/tests/logon_response_flags_drop_spooler_process_and_preserve_valid_bits
---

# Signature

`pub(in crate::mapi) fn rop_logon_response_body( principal: &AccountPrincipal, request: &RopRequest, ) -> Vec<u8>`

# Calls

- [private_logon_response_logon_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/private_logon_response_logon_flags.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [logon_time_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [gwart_time_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/gwart_time_marker.md)

# Called by

- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)
- [logon_response_debug_summary_decodes_private_mailbox_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields.md)
- [private_logon_places_exactly_13_folder_ids_before_response_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/private_logon_places_exactly_13_folder_ids_before_response_flags.md)
- [logon_response_flags_drop_spooler_process_and_preserve_valid_bits](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/logon_response_flags_drop_spooler_process_and_preserve_valid_bits.md)