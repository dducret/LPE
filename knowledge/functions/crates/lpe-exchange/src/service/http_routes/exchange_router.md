---
type: Rust Function
title: exchange_router
resource: crates/lpe-exchange/src/service/http_routes.rs#L25-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_paths
  called_by:
  - functions/crates/lpe-exchange/src/service/router
  - functions/crates/lpe-exchange/src/service/http_routes/exchange_router_builds_with_all_route_families
---

# Signature

`pub(super) fn exchange_router() -> Router<Storage>`

# Calls

- [rpc_proxy_paths](../../../../../../functions/crates/lpe-exchange/src/service/http_routes/rpc_proxy_paths.md)

# Called by

- [router](../../../../../../functions/crates/lpe-exchange/src/service/router.md)
- [exchange_router_builds_with_all_route_families](../../../../../../functions/crates/lpe-exchange/src/service/http_routes/exchange_router_builds_with_all_route_families.md)