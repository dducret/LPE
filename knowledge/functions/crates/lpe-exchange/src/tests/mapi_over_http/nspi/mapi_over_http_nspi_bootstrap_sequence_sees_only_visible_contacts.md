---
type: Rust Function
title: mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L1048-L1198
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/collection
  - functions/crates/lpe-exchange/src/tests/nspi_bound_headers
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts()`

# Calls

- [collection](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/collection.md)
- [nspi_bound_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/nspi_bound_headers.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [nspi_dn_to_mid_request](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)