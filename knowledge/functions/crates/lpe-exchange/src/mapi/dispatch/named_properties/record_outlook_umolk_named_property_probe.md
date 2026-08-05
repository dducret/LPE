---
type: Rust Function
title: record_outlook_umolk_named_property_probe
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L387-L450
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_create
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`fn record_outlook_umolk_named_property_probe( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, requested_count: usize, missing_count: usize, allocated_or_store_resolved_count: usize, unresolved_count: usize, legacy_low_dynamic_property_id_count: usize, returned_count: usize, duplicate_requested_count: usize, duplicate_returned_id_count: usize, returned_id_collision_count: usize, returned_id_collisions: &str, property_id_source_summary: &str, property_family_summary: &str, property_id_mapping_summary: &str, response_rop_payload_bytes: usize, )`

# Calls

- [named_property_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_create.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [is_outlook_umolk_user_options_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)