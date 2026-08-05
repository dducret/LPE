---
type: Rust Method
title: into_response
resource: LPE-CT/src/management_auth.rs#L34-L36
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/download_host_log
  - functions/LPE-CT/src/observability/metrics_endpoint
  - functions/crates/lpe-admin-api/src/client_config/jmap_well_known
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response
  - functions/crates/lpe-admin-api/src/client_config/xml_response
  - functions/crates/lpe-admin-api/src/observability/metrics_endpoint
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/service/options_handler
  - functions/crates/lpe-exchange/src/service/mapi_options_handler
  - functions/crates/lpe-exchange/src/service/rpc_proxy_handler
  - functions/crates/lpe-exchange/src/service/ews/xml/xml_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_accepted_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/service/api_concurrency_limit
  - functions/crates/lpe-jmap/src/service/upload_concurrency_limit
---

# Signature

`fn into_response(self) -> axum::response::Response`

# Called by

- [download_host_log](../../../../../../functions/LPE-CT/src/http_routes/download_host_log.md)
- [metrics_endpoint](../../../../../../functions/LPE-CT/src/observability/metrics_endpoint.md)
- [jmap_well_known](../../../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known.md)
- [outlook_autodiscover_post](../../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [render_autodiscover_json](../../../../../../functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json.md)
- [autodiscover_json_invalid_protocol_response](../../../../../../functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response.md)
- [xml_response](../../../../../../functions/crates/lpe-admin-api/src/client_config/xml_response.md)
- [metrics_endpoint](../../../../../../functions/crates/lpe-admin-api/src/observability/metrics_endpoint.md)
- [mapi_error_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response.md)
- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [options_handler](../../../../../../functions/crates/lpe-exchange/src/service/options_handler.md)
- [mapi_options_handler](../../../../../../functions/crates/lpe-exchange/src/service/mapi_options_handler.md)
- [rpc_proxy_handler](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)
- [xml_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/xml_response.md)
- [rpc_proxy_accepted_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_accepted_response.md)
- [rpc_proxy_auth_challenge_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response.md)
- [rpc_proxy_in_channel_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response.md)
- [rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)
- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [api_concurrency_limit](../../../../../../functions/crates/lpe-jmap/src/service/api_concurrency_limit.md)
- [upload_concurrency_limit](../../../../../../functions/crates/lpe-jmap/src/service/upload_concurrency_limit.md)