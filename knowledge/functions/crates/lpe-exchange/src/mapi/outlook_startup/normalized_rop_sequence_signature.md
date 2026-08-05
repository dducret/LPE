---
type: Rust Function
title: normalized_rop_sequence_signature
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L28-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/push_compressed_rop
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
---

# Signature

`pub(in crate::mapi) fn normalized_rop_sequence_signature(names_csv: &str) -> String`

# Calls

- [push_compressed_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/push_compressed_rop.md)

# Called by

- [log_execute_rop_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)