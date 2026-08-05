---
type: Rust Function
title: rpc_proxy_mailstore_held_open_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L83-L140
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/register_rpc_proxy_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_pending_rpc_proxy_out_channel_responses
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_should_send_synthetic_rts_connect
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/remove_rpc_proxy_out_channel
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect
---

# Signature

`fn rpc_proxy_mailstore_held_open_response( uri: &Uri, body: Vec<u8>, virtual_connection_cookie: Option<[u8; 16]>, ) -> Response`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)
- [register_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/register_rpc_proxy_out_channel.md)
- [consume_pending_rpc_proxy_out_channel_responses](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_pending_rpc_proxy_out_channel_responses.md)
- [rpc_proxy_should_send_synthetic_rts_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_should_send_synthetic_rts_connect.md)
- [rpc_proxy_connection_established_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu.md)
- [mark_rpc_proxy_out_endpoint_rts_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect.md)
- [rpc_proxy_dce_bind_ack_body_with_result_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count.md)
- [mark_rpc_proxy_out_endpoint_bind_ack](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack.md)
- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [remove_rpc_proxy_out_channel](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/remove_rpc_proxy_out_channel.md)
- [decorate_rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [rpc_proxy_mailstore_ping_response_for_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect.md)