---
type: Rust Function
title: rpc_proxy_nspi_get_matches_response_for_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L479-L507
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values_for_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum_with_store
---

# Signature

`async fn rpc_proxy_nspi_get_matches_response_for_principal<S>( store: &S, call_id: u32, request: &[u8], principal: &AccountPrincipal, ) -> Vec<u8> where S: ExchangeStore,`

# Calls

- [rpc_proxy_nspi_requested_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_property_tags.md)
- [rpc_proxy_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_address_book_entries.md)
- [rpc_proxy_filter_nspi_entries](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries.md)
- [rpc_proxy_nspi_row_values_for_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values_for_entry.md)
- [rpc_proxy_nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id.md)
- [rpc_proxy_push_stat](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_tag_array.md)
- [rpc_proxy_push_rowset_pointer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)

# Called by

- [rpc_proxy_nspi_response_for_opnum_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum_with_store.md)