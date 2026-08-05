---
type: Rust Function
title: rpc_proxy_address_book_entries
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L918-L929
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal
---

# Signature

`async fn rpc_proxy_address_book_entries<S>( store: &S, principal: &AccountPrincipal, ) -> Vec<ExchangeAddressBookEntry> where S: ExchangeStore,`

# Calls

- [fetch_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)

# Called by

- [rpc_proxy_nspi_query_rows_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal.md)
- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)
- [rpc_proxy_nspi_resolve_names_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_resolve_names_response_for_principal.md)