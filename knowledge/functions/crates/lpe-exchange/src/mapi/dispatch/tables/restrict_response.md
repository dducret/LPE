---
type: Rust Function
title: restrict_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1388-L1390
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_restrict_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
---

# Signature

`pub(super) fn restrict_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_restrict_response.md)

# Called by

- [append_restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)