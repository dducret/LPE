---
type: Rust Function
title: execute_success_rop_buffer
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L161-L168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(super) fn execute_success_rop_buffer(body: &[u8]) -> Option<&[u8]>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)