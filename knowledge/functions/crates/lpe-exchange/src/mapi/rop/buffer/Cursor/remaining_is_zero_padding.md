---
type: Rust Method
title: remaining_is_zero_padding
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L82-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request
---

# Signature

`pub(in crate::mapi) fn remaining_is_zero_padding(&self) -> bool`

# Calls

- [remaining](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [read_next_execute_rop_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request.md)