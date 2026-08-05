---
type: Rust Function
title: record_outlook_umolk_getprops_materialization
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L318-L406
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/classify_umolk_dictionary_shape
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
---

# Signature

`fn record_outlook_umolk_getprops_materialization( principal: &AccountPrincipal, session: &mut MapiSession, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, associated_config_debug: Option<&(String, String, String)>, property_tags: &[u32], property_response: &[u8], response_shape: &str, )`

# Calls

- [is_outlook_umolk_user_options_message_class](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class.md)
- [property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [summarize_flagged_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization.md)
- [classify_umolk_dictionary_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/classify_umolk_dictionary_shape.md)
- [summarize_umolk_roaming_dictionary_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract.md)
- [record_outlook_view_failure_trace_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [log_get_properties_specific_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)