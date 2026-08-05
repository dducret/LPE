---
type: Rust Function
title: mapi_over_http_long_term_id_round_trips_canonical_replica_ids
resource: crates/lpe-exchange/src/tests/mapi_over_http/connect.rs#L4497-L4653
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/append_mapi_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_long_term_id_round_trips_canonical_replica_ids()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [append_mapi_trailing_replid_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_trailing_replid_wire_id.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)