---
type: Rust Function
title: summarize_set_properties_probe_response
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes.rs#L257-L276
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
---

# Signature

`pub(in crate::mapi::dispatch) fn summarize_set_properties_probe_response( responses: &[u8], offset: usize, request: &SetPropertiesProbeRequest, ) -> String`

# Calls

- [read_response_error_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)