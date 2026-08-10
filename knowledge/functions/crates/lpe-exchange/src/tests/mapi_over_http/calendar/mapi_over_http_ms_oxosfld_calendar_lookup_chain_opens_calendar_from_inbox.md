---
type: Rust Function
title: mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L10162-L10249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_default_scoped_mapi_identity
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test
---

# Signature

`async fn mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [with_scoped_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity.md)
- [with_default_scoped_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_default_scoped_mapi_identity.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [durable_special_folder_id_for_test](../../../../../../../functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test.md)