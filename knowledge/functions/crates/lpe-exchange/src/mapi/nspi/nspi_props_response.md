---
type: Rust Function
title: nspi_props_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L573-L705
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/nspi_raw_property_tag_candidates
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_is_principal
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_missing_property_value_list
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) async fn nspi_props_response<S>( store: &S, principal: &AccountPrincipal, request: &[u8], request_type: &str, request_id: &str, ) -> Response where S: ExchangeStore,`

# Calls

- [fetch_address_book_entries](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [allocate_nspi_entry_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities.md)
- [allocate_principal_nspi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity.md)
- [parse_nspi_get_props_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request.md)
- [nspi_raw_property_tag_candidates](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/nspi_raw_property_tag_candidates.md)
- [nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_id.md)
- [resolve_names_requested_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
- [nspi_stat_current_rec](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [nspi_requested_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)
- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [nspi_lookup_matches_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_matches_principal.md)
- [nspi_request_has_entry_selector](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector.md)
- [nspi_entry_is_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_is_principal.md)
- [nspi_get_props_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags.md)
- [nspi_property_tag_is_supported](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported.md)
- [nspi_get_props_property_value_list](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_value_list.md)
- [nspi_get_props_missing_property_value_list](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_missing_property_value_list.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [log_nspi_get_props_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)