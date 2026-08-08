---
type: Rust Function
title: resolve_names_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L204-L295
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_columns
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
---

# Signature

`pub(in crate::mapi) async fn resolve_names_response<S>( store: &S, principal: &AccountPrincipal, request: &[u8], request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [resolve_names_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_columns.md)
- [resolve_names_requested_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
- [fetch_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [allocate_nspi_entry_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities.md)
- [allocate_principal_nspi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity.md)
- [nspi_match_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_entry.md)
- [nspi_lookup_matches_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_large_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array.md)
- [nspi_resolved_entry_row](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)