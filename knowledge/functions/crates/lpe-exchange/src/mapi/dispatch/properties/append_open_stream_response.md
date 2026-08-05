---
type: Rust Function
title: append_open_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L529-L708
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_debug_fields
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_property_tag
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_open
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_open_mode
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_stream_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
---

# Signature

`pub(super) async fn append_open_stream_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_object_debug_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [associated_config_debug_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_debug_fields.md)
- [stream_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_property_tag.md)
- [record_inbox_associated_config_stream_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_open.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [normalize_named_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [open_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data.md)
- [stream_open_mode](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_open_mode.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [record_inbox_associated_config_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_handle.md)
- [record_inbox_rule_organizer_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_stream_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)