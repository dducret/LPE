---
type: Rust Function
title: rpc_proxy_requested_nspi_entry
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1027-L1043
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_mids
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
---

# Signature

`fn rpc_proxy_requested_nspi_entry<'a>( entries: &'a [ExchangeAddressBookEntry], request: &[u8], ) -> Option<&'a ExchangeAddressBookEntry>`

# Calls

- [rpc_proxy_nspi_requested_mids](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_mids.md)
- [rpc_proxy_nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id.md)
- [rpc_proxy_nspi_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)
- [rpc_proxy_match_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry.md)

# Called by

- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)