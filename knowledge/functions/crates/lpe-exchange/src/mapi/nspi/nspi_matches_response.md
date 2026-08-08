---
type: Rust Function
title: nspi_matches_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L893-L970
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_response_contract
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) async fn nspi_matches_response<S>( store: &S, principal: &AccountPrincipal, request: &[u8], request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [fetch_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)
- [nspi_filter_entries_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request.md)
- [allocate_nspi_entry_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities.md)
- [nspi_requested_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_requested_property_tags.md)
- [log_nspi_rowset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [write_large_property_tag_array](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_large_property_tag_array.md)
- [nspi_resolved_entry_row](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_resolved_entry_row.md)
- [log_nspi_response_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_response_contract.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)