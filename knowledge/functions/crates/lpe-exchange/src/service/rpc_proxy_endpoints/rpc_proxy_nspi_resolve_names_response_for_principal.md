---
type: Rust Function
title: rpc_proxy_nspi_resolve_names_response_for_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L674-L752
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_principal_address_book_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_principal_matches
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum_with_store
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback
---

# Signature

`pub(super) async fn rpc_proxy_nspi_resolve_names_response_for_principal<S>( store: &S, call_id: u32, request: &[u8], principal: &AccountPrincipal, ) -> Vec<u8> where S: ExchangeStore,`

# Calls

- [rpc_proxy_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries.md)
- [rpc_proxy_principal_address_book_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_principal_address_book_entry.md)
- [rpc_proxy_nspi_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_lookup_values.md)
- [rpc_proxy_match_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry.md)
- [rpc_proxy_nspi_principal_matches](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_principal_matches.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)
- [rpc_proxy_nspi_requested_resolve_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_resolve_property_tags.md)
- [rpc_proxy_push_ndr_ascii_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string.md)

# Called by

- [rpc_proxy_nspi_response_for_opnum_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum_with_store.md)
- [rpc_proxy_address_book_check_name_fallback](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback.md)