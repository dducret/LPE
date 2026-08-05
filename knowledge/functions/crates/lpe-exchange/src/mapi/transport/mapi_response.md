---
type: Rust Function
title: mapi_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L673-L682
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_u32_result_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_update_stat_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_debug_retains_logical_payload_for_outlook_trace
  - functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_start_time_uses_current_http_date_not_sentinel
  - functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_uses_one_exchange_chunked_processing_and_done_frame
  - functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_responses_advertise_the_default_pending_period
---

# Signature

`pub(in crate::mapi) fn mapi_response( request_type: &str, request_id: &str, response_code: u16, body: Vec<u8>, cookie: Option<String>, ) -> Response`

# Calls

- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)

# Called by

- [endpoint_url_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response.md)
- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_u32_result_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_u32_result_response.md)
- [nspi_dn_to_mid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_get_prop_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
- [nspi_template_info_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
- [nspi_update_stat_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_update_stat_response.md)
- [nspi_property_tags_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tags_response.md)
- [nspi_hierarchy_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response.md)
- [ping_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [mapi_response_debug_retains_logical_payload_for_outlook_trace](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_debug_retains_logical_payload_for_outlook_trace.md)
- [mapi_response_start_time_uses_current_http_date_not_sentinel](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_start_time_uses_current_http_date_not_sentinel.md)
- [execute_response_uses_one_exchange_chunked_processing_and_done_frame](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_uses_one_exchange_chunked_processing_and_done_frame.md)
- [mapi_responses_advertise_the_default_pending_period](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_responses_advertise_the_default_pending_period.md)