---
type: Rust Function
title: mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L1718-L1825
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)
- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)