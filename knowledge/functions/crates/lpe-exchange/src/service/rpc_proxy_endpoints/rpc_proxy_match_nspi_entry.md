---
type: Rust Function
title: rpc_proxy_match_nspi_entry
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1045-L1060
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry
---

# Signature

`fn rpc_proxy_match_nspi_entry<'a>( entries: &'a [ExchangeAddressBookEntry], value: &str, ) -> Option<&'a ExchangeAddressBookEntry>`

# Calls

- [rpc_proxy_nspi_entry_matches](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches.md)
- [rpc_proxy_nspi_entry_exact_match](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match.md)

# Called by

- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)
- [rpc_proxy_requested_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry.md)