---
type: Rust Function
title: mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L15646-L15840
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/tests/append_mapi_i64_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/strict_test_replguid_globset
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/mapi_binary_property_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state()`

# Calls

- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [reserve_mapi_local_replica_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [append_mapi_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i64_property.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [strict_test_replguid_globset](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_test_replguid_globset.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_fast_transfer_chunks](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [mapi_binary_property_value](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_binary_property_value.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)