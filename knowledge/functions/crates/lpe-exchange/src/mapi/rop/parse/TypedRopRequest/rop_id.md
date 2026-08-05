---
type: Rust Method
title: rop_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1310-L1326
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
---

# Signature

`pub(in crate::mapi) fn rop_id(&self) -> u8`

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [summarize_request_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)