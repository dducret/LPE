---
type: Rust Function
title: read_next_execute_rop_request
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L334-L348
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining_is_zero_padding
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_parse_error_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn read_next_execute_rop_request( cursor: &mut Cursor<'_>, responses: &mut Vec<u8>, ) -> Option<(RopRequest, u8)>`

# Calls

- [remaining_is_zero_padding](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining_is_zero_padding.md)
- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [rop_parse_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_parse_error_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)