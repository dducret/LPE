---
type: Rust Function
title: log_message_getprops_response_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/message.rs#L49-L153
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_message_getprops_response_debug( principal: &AccountPrincipal, session: &mut MapiSession, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_response: &[u8], )`

# Calls

- [search_folder_message_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [summarize_message_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization.md)
- [record_outlook_view_failure_trace_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)