---
type: Rust Method
title: handle_rpc_proxy
resource: crates/lpe-exchange/src/service/mapi_http.rs#L17-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_ping
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_echo_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_echo_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_accepted_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_handler
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_missing_authentication_with_basic
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_anonymous_msrpch_echo_ping
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_authenticated_msrpch_echo_ping
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_accepts_authenticated_rca_probe_without_405
---

# Signature

`pub(crate) async fn handle_rpc_proxy( &self, method: &Method, uri: &Uri, headers: &HeaderMap, request_body: &[u8], ) -> Response`

# Calls

- [authenticate_account](../../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [parse_rpc_proxy_out_data_connect_request](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request.md)
- [is_rpc_proxy_endpoint_ping](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_endpoint_ping.md)
- [rpc_proxy_mailstore_ping_response_for_connect](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect.md)
- [rpc_proxy_rts_connect_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response.md)
- [is_rpc_proxy_echo_request](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_echo_request.md)
- [rpc_proxy_echo_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_echo_response.md)
- [rpc_proxy_accepted_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_accepted_response.md)
- [rpc_proxy_auth_challenge_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response.md)

# Called by

- [rpc_proxy_handler](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)
- [rpc_proxy_challenges_missing_authentication_with_basic](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_missing_authentication_with_basic.md)
- [rpc_proxy_challenges_anonymous_msrpch_echo_ping](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_challenges_anonymous_msrpch_echo_ping.md)
- [rpc_proxy_answers_authenticated_msrpch_echo_ping](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_authenticated_msrpch_echo_ping.md)
- [rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack.md)
- [rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first.md)
- [rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first.md)
- [rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack.md)
- [rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe.md)
- [rpc_proxy_accepts_authenticated_rca_probe_without_405](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_accepts_authenticated_rca_probe_without_405.md)