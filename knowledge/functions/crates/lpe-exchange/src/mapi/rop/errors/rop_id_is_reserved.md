---
type: Rust Function
title: rop_id_is_reserved
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L34-L36
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/wire/RopId/is_reserved
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
---

# Signature

`pub(in crate::mapi) fn rop_id_is_reserved(rop_id: u8) -> bool`

# Calls

- [is_reserved](../../../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/is_reserved.md)

# Called by

- [typed](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)