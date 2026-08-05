---
type: Rust Function
title: nspi_principal_mid
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L151-L161
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/test_account_legacy_dn
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request
  - functions/crates/lpe-exchange/src/tests/nspi_bound_headers
  - functions/crates/lpe-exchange/src/tests/response_bytes
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list
---

# Signature

`async fn nspi_principal_mid(service: &ExchangeService<FakeStore>) -> u32`

# Calls

- [test_account_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/test_account_legacy_dn.md)
- [nspi_dn_to_mid_request](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request.md)
- [nspi_bound_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/nspi_bound_headers.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)

# Called by

- [mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_one_error_for_one_missing_property.md)
- [mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_returns_error_for_missing_null_slot.md)
- [mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_get_props_encodes_missing_null_tag_as_error.md)
- [mapi_over_http_nspi_null_get_props_matches_entry_prop_list](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list.md)