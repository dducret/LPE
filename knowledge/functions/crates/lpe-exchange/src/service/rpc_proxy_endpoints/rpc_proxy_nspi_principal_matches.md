---
type: Rust Function
title: rpc_proxy_nspi_principal_matches
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1069-L1078
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
---

# Signature

`fn rpc_proxy_nspi_principal_matches(value: &str, principal: &AccountPrincipal) -> bool`

# Calls

- [rpc_proxy_normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)

# Called by

- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)