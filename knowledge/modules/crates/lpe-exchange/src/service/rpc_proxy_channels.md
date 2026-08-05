---
type: Rust Module
title: rpc_proxy_channels
resource: crates/lpe-exchange/src/service/rpc_proxy_channels.rs#L1-L241
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/std-sync-mutex-oncelock
  - external/std-time-duration-instant
  - external/axum-body-bytes
  - external/super-rpc-proxy-connection-timeout-ms
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rpc_proxy_channel_hold_ms](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_channel_hold_ms.md)
- [rpc_proxy_channel_hold_ms](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_channel_hold_ms-2.md)
- [mark_rpc_proxy_out_endpoint_bind_ack](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack.md)
- [consume_rpc_proxy_out_endpoint_bind_ack](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_bind_ack.md)
- [rpc_proxy_should_send_synthetic_rts_connect](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_should_send_synthetic_rts_connect.md)
- [register_rpc_proxy_out_channel](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/register_rpc_proxy_out_channel.md)
- [send_rpc_proxy_out_channel](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/send_rpc_proxy_out_channel.md)
- [queue_pending_rpc_proxy_out_channel_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response.md)
- [consume_pending_rpc_proxy_out_channel_responses](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_pending_rpc_proxy_out_channel_responses.md)
- [mark_rpc_proxy_out_endpoint_rts_connect](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect.md)
- [consume_rpc_proxy_out_endpoint_rts_connect](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_rts_connect.md)
- [remove_rpc_proxy_out_channel](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/remove_rpc_proxy_out_channel.md)
- [rpc_proxy_out_endpoint_bind_acks](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_bind_acks.md)
- [rpc_proxy_out_endpoint_rts_connects](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_endpoint_rts_connects.md)
- [pending_rpc_proxy_out_channel_responses](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/pending_rpc_proxy_out_channel_responses.md)
- [rpc_proxy_out_channels](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels.md)
- [rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie.md)
- [rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel.md)

# Imports

- `std::collections::HashMap`
- `std::sync::{Mutex, OnceLock}`
- `std::time::{Duration, Instant}`
- `axum::body::Bytes`
- `super::RPC_PROXY_CONNECTION_TIMEOUT_MS`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)