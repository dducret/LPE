---
type: Rust Function
title: append_get_properties_specific_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L84-L449
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_folder_type_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_open_loop_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_config
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/should_log_outlook_surface_getprops_info
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_folder_type_getprops_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_post_fai_folder_type_probe_loop_context
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_folder_type_probe_loop_logged
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response
---

# Signature

`pub(super) async fn append_get_properties_specific_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], created_emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, response_size_limit: usize, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [record_inbox_folder_type_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_folder_type_getprops_probe.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [format_inbox_open_loop_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_open_loop_summary.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [attachment_overlay_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object.md)
- [fetch_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request.md)
- [format_debug_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [normalized_get_properties_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [log_message_getprops_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug.md)
- [log_get_properties_specific_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)
- [log_get_properties_view_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug.md)
- [log_get_properties_default_folder_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)
- [property_ids_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match.md)
- [default_folder_named_view_config](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_config.md)
- [record_default_view_advertised](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised.md)
- [log_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint.md)
- [post_hierarchy_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract.md)
- [format_outlook_surface_folder_getprops_trace](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace.md)
- [should_log_outlook_surface_getprops_info](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/should_log_outlook_surface_getprops_info.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [record_post_hierarchy_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_getprops_contract.md)
- [record_post_hierarchy_request_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_receive_folder_verification_passed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [record_last_inbox_folder_type_getprops_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_folder_type_getprops_context.md)
- [format_post_fai_folder_type_probe_loop_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_post_fai_folder_type_probe_loop_context.md)
- [record_mapi_outlook_view_bootstrap_stall](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall.md)
- [mark_post_inbox_fai_folder_type_probe_loop_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_folder_type_probe_loop_logged.md)

# Called by

- [append_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response.md)