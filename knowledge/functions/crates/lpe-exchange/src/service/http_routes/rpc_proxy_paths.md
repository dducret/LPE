---
type: Rust Function
title: rpc_proxy_paths
resource: crates/lpe-exchange/src/service/http_routes.rs#L21-L23
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/http_routes/exchange_router
  - functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_routes_include_outlook_canonical_case
---

# Signature

`pub(super) fn rpc_proxy_paths() -> [&'static str; 2]`

# Called by

- [exchange_router](../../../../../../functions/crates/lpe-exchange/src/service/http_routes/exchange_router.md)
- [rpc_proxy_routes_include_outlook_canonical_case](../../../../../../functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_routes_include_outlook_canonical_case.md)