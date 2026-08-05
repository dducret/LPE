---
type: Rust Module
title: http_routes
resource: crates/lpe-exchange/src/service/http_routes.rs#L1-L75
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-routing-any-on-methodfilter-router
  - external/lpe-storage-storage
  - external/super-mapi-emsmdb-post-handler-mapi-nspi-post-handler-mapi-options-handler-options-handler-post-handler-rpc-proxy-handler
  - external/super-exchange-router-rpc-proxy-paths-rpc-proxy-outlook-canonical-path-rpc-proxy-path
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rpc_proxy_paths](../../../../../functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_paths.md)
- [exchange_router](../../../../../functions/crates/lpe-exchange/src/service/http_routes/exchange_router.md)
- [rpc_proxy_routes_include_outlook_canonical_case](../../../../../functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_routes_include_outlook_canonical_case.md)
- [exchange_router_builds_with_all_route_families](../../../../../functions/crates/lpe-exchange/src/service/http_routes/exchange_router_builds_with_all_route_families.md)

# Imports

- `axum::{
    routing::{any, on, MethodFilter},
    Router,
}`
- `lpe_storage::Storage`
- `super::{
    mapi_emsmdb_post_handler, mapi_nspi_post_handler, mapi_options_handler, options_handler,
    post_handler, rpc_proxy_handler,
}`
- `super::{
        exchange_router, rpc_proxy_paths, RPC_PROXY_OUTLOOK_CANONICAL_PATH, RPC_PROXY_PATH,
    }`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)