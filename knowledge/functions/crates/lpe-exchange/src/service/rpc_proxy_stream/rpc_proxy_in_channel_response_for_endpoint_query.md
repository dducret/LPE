---
type: Rust Function
title: rpc_proxy_in_channel_response_for_endpoint_query
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L456-L498
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_buffer
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_management_stats_accepts_rca_short_stub
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_referral_opnums_get_server_name_responses
---

# Signature

`pub(crate) fn rpc_proxy_in_channel_response_for_endpoint_query( endpoint_query: &str, buffer: &mut Vec<u8>, ) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rpc_proxy_conn_b1_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body.md)
- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)

# Called by

- [rpc_proxy_in_channel_response_for_buffer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_buffer.md)
- [rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic.md)
- [rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response.md)
- [rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response.md)
- [rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context.md)
- [rpc_proxy_mailstore_management_stats_accepts_rca_short_stub](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_management_stats_accepts_rca_short_stub.md)
- [rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack.md)
- [rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe.md)
- [rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id.md)
- [rpc_proxy_in_channel_referral_opnums_get_server_name_responses](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_referral_opnums_get_server_name_responses.md)