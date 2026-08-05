---
type: Rust Module
title: rpc_proxy
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1-L1749
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [mapi_over_http_rejects_missing_authentication](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/mapi_over_http_rejects_missing_authentication.md)
- [rpc_proxy_challenges_missing_authentication_with_basic](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_missing_authentication_with_basic.md)
- [rpc_proxy_challenges_anonymous_msrpch_echo_ping](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_anonymous_msrpch_echo_ping.md)
- [rpc_proxy_answers_authenticated_msrpch_echo_ping](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_authenticated_msrpch_echo_ping.md)
- [rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first.md)
- [rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first.md)
- [rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack.md)
- [rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof.md)
- [rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof.md)
- [rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body.md)
- [rpc_proxy_classifies_referral_endpoint_as_streaming_in_data_channel](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_classifies_referral_endpoint_as_streaming_in_data_channel.md)
- [rpc_proxy_classifies_zero_length_endpoint_in_data_as_echo_probe](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_classifies_zero_length_endpoint_in_data_as_echo_probe.md)
- [rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe.md)
- [rpc_proxy_in_channel_endpoint_ping_request_gets_success_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_endpoint_ping_request_gets_success_response.md)
- [rpc_proxy_in_channel_bind_request_gets_bind_ack_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_request_gets_bind_ack_response.md)
- [rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features.md)
- [rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic.md)
- [rpc_proxy_in_channel_alter_context_request_gets_alter_context_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_alter_context_request_gets_alter_context_response.md)
- [rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response.md)
- [rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response.md)
- [rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context.md)
- [rpc_proxy_mailstore_management_stats_accepts_rca_short_stub](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_management_stats_accepts_rca_short_stub.md)
- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault.md)
- [rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context.md)
- [rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack.md)
- [rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe.md)
- [rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response.md)
- [rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response.md)
- [rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response.md)
- [rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id.md)
- [rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch.md)
- [rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback.md)
- [rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu.md)
- [rpc_proxy_in_channel_nspi_unbind_request_gets_success_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_unbind_request_gets_success_response.md)
- [rpc_proxy_address_book_management_stats_accepts_rca_short_stub](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_management_stats_accepts_rca_short_stub.md)
- [rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses.md)
- [rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response.md)
- [rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response.md)
- [rpc_proxy_in_channel_referral_opnums_get_server_name_responses](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_referral_opnums_get_server_name_responses.md)
- [rpc_proxy_referral_get_fqdn_accepts_rca_short_stub](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_get_fqdn_accepts_rca_short_stub.md)
- [nspi_rpc_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/nspi_rpc_request.md)
- [rfri_rpc_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rfri_rpc_request.md)
- [emsmdb_rpc_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_request.md)
- [emsmdb_rpc_ext2_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_ext2_request.md)
- [rpc_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request.md)
- [rpc_response_context](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_context.md)
- [rpc_response_call_id](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_call_id.md)
- [rpc_response_fault_status](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_fault_status.md)
- [rpc_response_rpc_header_ext](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_rpc_header_ext.md)
- [rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment.md)
- [rpc_proxy_in_channel_buffers_split_endpoint_ping_request](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_buffers_split_endpoint_ping_request.md)
- [rpc_proxy_accepts_authenticated_rca_probe_without_405](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_accepts_authenticated_rca_probe_without_405.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)