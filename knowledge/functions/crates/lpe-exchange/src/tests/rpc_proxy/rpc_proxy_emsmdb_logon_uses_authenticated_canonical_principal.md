---
type: Rust Function
title: rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L793-L831
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeDetector/pdf
  - functions/crates/lpe-exchange/src/tests/test_account_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_context
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_bootstrap_logon_execute_rop
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_ext2_request
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_rpc_header_ext
---

# Signature

`async fn rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal()`

# Calls

- [pdf](../../../../../../functions/crates/lpe-exchange/src/tests/FakeDetector/pdf.md)
- [test_account_principal](../../../../../../functions/crates/lpe-exchange/src/tests/test_account_principal.md)
- [emsmdb_rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_request.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [rpc_response_context](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_context.md)
- [rpc_proxy_bootstrap_logon_execute_rop](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_bootstrap_logon_execute_rop.md)
- [emsmdb_rpc_ext2_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_ext2_request.md)
- [rpc_response_rpc_header_ext](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_response_rpc_header_ext.md)