---
type: Rust Function
title: mapi_over_http_public_folder_modify_permissions_requires_share_right
resource: crates/lpe-exchange/src/tests/mapi_over_http/public_folders.rs#L1798-L1910
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/public_folder
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_public_folder_modify_permissions_requires_share_right()`

# Calls

- [public_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/public_folder.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rights_from_grant](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)