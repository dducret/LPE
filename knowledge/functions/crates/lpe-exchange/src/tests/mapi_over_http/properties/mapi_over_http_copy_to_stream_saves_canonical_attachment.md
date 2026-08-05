---
type: Rust Function
title: mapi_over_http_copy_to_stream_saves_canonical_attachment
resource: crates/lpe-exchange/src/tests/mapi_over_http/properties.rs#L3222-L3397
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeDetector/pdf
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/test_mapi_message_id
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`async fn mapi_over_http_copy_to_stream_saves_canonical_attachment()`

# Calls

- [pdf](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeDetector/pdf.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [test_mapi_message_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_message_id.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)