---
type: Rust Function
title: mapi_over_http_contact_crud_uses_canonical_contacts
resource: crates/lpe-exchange/src/tests/mapi_over_http/contacts.rs#L833-L977
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_create_message
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/saved_message_id_from_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_open_message
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/tests/append_rop_delete_messages
---

# Signature

`async fn mapi_over_http_contact_crud_uses_canonical_contacts()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_create_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_message.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [saved_message_id_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/saved_message_id_from_response.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_open_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_message.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [append_rop_delete_messages](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_delete_messages.md)