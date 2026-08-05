---
type: Rust Function
title: mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags
resource: crates/lpe-exchange/src/tests/mapi_over_http/contacts.rs#L1434-L1532
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_rop_create_associated_message
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_rop_create_associated_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_associated_message.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)