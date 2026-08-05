---
type: Rust Module
title: rpc_proxy_stream
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L1-L852
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-time-duration-instant
  - external/anyhow-result
  - external/axum-body-body-bytes-http-header-connection-content-length-content-type-headermap-headervalue-method-statuscode-uri-response-intoresponse-response
  - external/lpe-magika-detector-validator
  - external/lpe-mail-auth-accountprincipal
  - external/tokio-stream-wrappers-receiverstream-streamext
  - external/tracing-info-warn
  - external/crate-mapi-store-exchangestore
  - external/super-rpc-proxy-channels
  - external/super-rpc-proxy-codec-read-le-u32
  - external/super-rpc-proxy-dce
  - external/super-rpc-proxy-endpoints
  - external/super-rpc-proxy-requests-is-rpc-proxy-endpoint-query
  - external/super-rpc-proxy-rts
  - external/super-transport-diagnostics-rpcproxyresponsedebug-rpcproxyresponsepayloadpreview
  - external/super-rpc-proxy-compat-status-rpc-proxy-receive-window-size
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rpc_proxy_rts_connect_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response.md)
- [rpc_proxy_mailstore_ping_response_for_connect](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect.md)
- [rpc_proxy_echo_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_echo_response.md)
- [rpc_proxy_in_channel_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)
- [rpc_proxy_mailstore_held_open_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
- [should_hold_rpc_proxy_in_channel](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/should_hold_rpc_proxy_in_channel.md)
- [rpc_proxy_binary_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)
- [rpc_proxy_held_open_binary_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_held_open_binary_response.md)
- [decorate_rpc_proxy_binary_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/decorate_rpc_proxy_binary_response.md)
- [spawn_rpc_proxy_in_data_drain](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/spawn_rpc_proxy_in_data_drain.md)
- [log_and_forward_rpc_proxy_in_channel_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/log_and_forward_rpc_proxy_in_channel_response.md)
- [rpc_proxy_in_channel_response_for_buffer](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_buffer.md)
- [rpc_proxy_in_channel_response_for_endpoint_query](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query.md)
- [rpc_proxy_address_book_check_name_fallback](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback.md)
- [rpc_proxy_last_dce_request_call_id](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_last_dce_request_call_id.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response.md)
- [rpc_proxy_endpoint_response_for_fragment](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)

# Imports

- `std::time::{Duration, Instant}`
- `anyhow::Result`
- `axum::{
    body::{Body, Bytes},
    http::{
        header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE},
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
}`
- `lpe_magika::{Detector, Validator}`
- `lpe_mail_auth::AccountPrincipal`
- `tokio_stream::{wrappers::ReceiverStream, StreamExt}`
- `tracing::{info, warn}`
- `crate::{mapi, store::ExchangeStore}`
- `super::rpc_proxy_channels::*`
- `super::rpc_proxy_codec::read_le_u32`
- `super::rpc_proxy_dce::*`
- `super::rpc_proxy_endpoints::*`
- `super::rpc_proxy_requests::is_rpc_proxy_endpoint_query`
- `super::rpc_proxy_rts::*`
- `super::transport_diagnostics::{RpcProxyResponseDebug, RpcProxyResponsePayloadPreview}`
- `super::{RPC_PROXY_COMPAT_STATUS, RPC_PROXY_RECEIVE_WINDOW_SIZE}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)