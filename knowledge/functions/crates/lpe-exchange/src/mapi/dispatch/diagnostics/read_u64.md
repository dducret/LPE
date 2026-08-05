---
type: Rust Function
title: read_u64
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L859-L862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
---

# Signature

`fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)