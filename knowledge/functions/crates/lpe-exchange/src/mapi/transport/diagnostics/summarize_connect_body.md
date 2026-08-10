---
type: Rust Function
title: summarize_connect_body
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L55-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_connect_body_debug
  - functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields
---

# Signature

`pub(in crate::mapi) fn summarize_connect_body(body: &[u8]) -> ConnectBodyDebugSummary`

# Calls

- [read_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [log_connect_body_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_connect_body_debug.md)
- [connect_body_debug_summary_decodes_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields.md)