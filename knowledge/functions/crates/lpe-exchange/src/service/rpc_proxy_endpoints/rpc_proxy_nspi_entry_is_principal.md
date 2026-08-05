---
type: Rust Function
title: rpc_proxy_nspi_entry_is_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1062-L1067
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
---

# Signature

`fn rpc_proxy_nspi_entry_is_principal( entry: &ExchangeAddressBookEntry, principal: &AccountPrincipal, ) -> bool`

# Called by

- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)