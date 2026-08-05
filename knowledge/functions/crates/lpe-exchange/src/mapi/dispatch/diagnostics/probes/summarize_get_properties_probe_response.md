---
type: Rust Function
title: summarize_get_properties_probe_response
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes.rs#L213-L233
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn summarize_get_properties_probe_response( responses: &[u8], offset: usize, request: &GetPropertiesSpecificProbeRequest, ) -> String`

# Calls

- [read_response_error_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [summarize_get_properties_probe_response_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response_values.md)

# Called by

- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [log_get_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)
- [log_get_properties_specific_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)