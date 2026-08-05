---
type: Rust Function
title: rpc_proxy_in_channel_response_for_buffer
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L451-L453
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_endpoint_ping_request_gets_success_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_request_gets_bind_ack_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_alter_context_request_gets_alter_context_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_unbind_request_gets_success_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_buffers_split_endpoint_ping_request
---

# Signature

`pub(crate) fn rpc_proxy_in_channel_response_for_buffer(buffer: &mut Vec<u8>) -> Option<Vec<u8>>`

# Calls

- [rpc_proxy_in_channel_response_for_endpoint_query](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query.md)

# Called by

- [rpc_proxy_in_channel_endpoint_ping_request_gets_success_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_endpoint_ping_request_gets_success_response.md)
- [rpc_proxy_in_channel_bind_request_gets_bind_ack_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_request_gets_bind_ack_response.md)
- [rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features.md)
- [rpc_proxy_in_channel_alter_context_request_gets_alter_context_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_alter_context_request_gets_alter_context_response.md)
- [rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response.md)
- [rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response.md)
- [rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response.md)
- [rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu.md)
- [rpc_proxy_in_channel_nspi_unbind_request_gets_success_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_unbind_request_gets_success_response.md)
- [rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses.md)
- [rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response.md)
- [rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response.md)
- [rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment.md)
- [rpc_proxy_in_channel_buffers_split_endpoint_ping_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_buffers_split_endpoint_ping_request.md)