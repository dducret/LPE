---
type: Rust Method
title: handle_rpc_proxy_in_data_channel
resource: crates/lpe-exchange/src/service/mapi_http.rs#L44-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_handler
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body
---

# Signature

`pub(crate) async fn handle_rpc_proxy_in_data_channel( &self, method: &Method, uri: &Uri, headers: &HeaderMap, body: Body, ) -> Response`

# Calls

- [authenticate_account](../../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [spawn_rpc_proxy_in_data_drain](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain.md)
- [rpc_proxy_in_channel_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)
- [rpc_proxy_auth_challenge_response](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response.md)

# Called by

- [rpc_proxy_handler](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)
- [rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first.md)
- [rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first.md)
- [rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack.md)
- [rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof.md)
- [rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof.md)
- [rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body](../../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body.md)