---
type: Rust Function
title: read_le_u32_at
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L706-L709
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_parse_failure_debug
---

# Signature

`fn read_le_u32_at(bytes: &[u8], offset: usize) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [log_execute_parse_failure_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_parse_failure_debug.md)