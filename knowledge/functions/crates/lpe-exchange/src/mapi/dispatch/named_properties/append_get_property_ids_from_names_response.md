---
type: Rust Function
title: append_get_property_ids_from_names_response
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L94-L385
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_names
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/named_properties_for_query
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_create
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/cache_named_property_mapping_and_return_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/unresolved_named_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_id_sources
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_family_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_resolution_mappings
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/legacy_low_dynamic_property_id_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/contains_outlook_osc_contact_source_probe
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/contains_outlook_view_descriptor_probe
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response
---

# Signature

`pub(super) async fn append_get_property_ids_from_names_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [named_property_names](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_names.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [fetch_mapi_named_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties.md)
- [cache_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property.md)
- [named_properties_for_query](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/named_properties_for_query.md)
- [rop_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_property_ids_from_names_response.md)
- [format_debug_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_properties.md)
- [normalize_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property.md)
- [property_id_for_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [fetch_or_allocate_mapi_named_property_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids.md)
- [named_property_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_create.md)
- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [cache_named_property_mapping_and_return_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/cache_named_property_mapping_and_return_property_id.md)
- [summarize_named_property_id_duplicates](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates.md)
- [unresolved_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/unresolved_named_properties.md)
- [format_named_property_id_sources](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_id_sources.md)
- [format_named_property_family_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_family_summary.md)
- [format_named_property_resolution_mappings](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_resolution_mappings.md)
- [legacy_low_dynamic_property_id_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/legacy_low_dynamic_property_id_count.md)
- [record_outlook_umolk_named_property_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe.md)
- [contains_outlook_osc_contact_source_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/contains_outlook_osc_contact_source_probe.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [contains_outlook_view_descriptor_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/contains_outlook_view_descriptor_probe.md)

# Called by

- [append_named_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response.md)