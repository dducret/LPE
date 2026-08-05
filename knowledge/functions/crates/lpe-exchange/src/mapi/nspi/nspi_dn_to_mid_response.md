---
type: Rust Function
title: nspi_dn_to_mid_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L375-L446
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_dn_to_mid_debug
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) async fn nspi_dn_to_mid_response<S>( store: &S, principal: &AccountPrincipal, request: &[u8], request_type: &str, request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [fetch_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [allocate_nspi_entry_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities.md)
- [allocate_principal_nspi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity.md)
- [parse_dn_to_mid_names](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [nspi_dn_to_mid_match](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_match.md)
- [log_nspi_dn_to_mid_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_dn_to_mid_debug.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)