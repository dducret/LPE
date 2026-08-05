---
type: Rust Function
title: rpc_proxy_nspi_utf16_lookup_values
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1145-L1174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values
---

# Signature

`fn rpc_proxy_nspi_utf16_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rpc_proxy_normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)

# Called by

- [rpc_proxy_nspi_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)