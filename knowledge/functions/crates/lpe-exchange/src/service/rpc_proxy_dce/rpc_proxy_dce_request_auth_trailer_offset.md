---
type: Rust Function
title: rpc_proxy_dce_request_auth_trailer_offset
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L287-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_auth_trailer_candidate
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth
---

# Signature

`fn rpc_proxy_dce_request_auth_trailer_offset( request: &[u8], fragment_length: usize, auth_length: usize, ) -> Option<usize>`

# Calls

- [rpc_proxy_dce_auth_trailer_candidate](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_auth_trailer_candidate.md)

# Called by

- [rpc_proxy_dce_request_auth](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth.md)