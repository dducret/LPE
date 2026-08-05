---
type: Rust Function
title: read_guid_le
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L864-L867
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

`fn read_guid_le(cursor: &mut Cursor<'_>) -> Result<String>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)