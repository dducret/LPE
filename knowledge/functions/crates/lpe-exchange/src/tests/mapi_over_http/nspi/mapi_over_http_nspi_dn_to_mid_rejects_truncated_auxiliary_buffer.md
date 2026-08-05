---
type: Rust Function
title: mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L278-L286
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/test_account_legacy_dn
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/assert_nspi_dn_to_mid_request_rejected
---

# Signature

`async fn mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer()`

# Calls

- [test_account_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/test_account_legacy_dn.md)
- [nspi_dn_to_mid_request](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [assert_nspi_dn_to_mid_request_rejected](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/assert_nspi_dn_to_mid_request_rejected.md)