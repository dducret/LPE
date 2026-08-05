---
type: Rust Function
title: rpc_proxy_nspi_ascii_lookup_values
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1131-L1143
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values
---

# Signature

`fn rpc_proxy_nspi_ascii_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [rpc_proxy_normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)

# Called by

- [rpc_proxy_nspi_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)