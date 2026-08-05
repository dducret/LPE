---
type: Rust Function
title: mapi_over_http_default_contacts_folder_properties_use_persisted_change_number
resource: crates/lpe-exchange/src/tests/mapi_over_http/hierarchy.rs#L4-L123
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset
  - functions/crates/lpe-exchange/src/tests/read_rop_binary_u16
---

# Signature

`async fn mapi_over_http_default_contacts_folder_properties_use_persisted_change_number()`

# Calls

- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [virtual_special_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_get_properties_specific_standard_row_offset](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset.md)
- [read_rop_binary_u16](../../../../../../../functions/crates/lpe-exchange/src/tests/read_rop_binary_u16.md)