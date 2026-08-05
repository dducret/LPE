---
type: Rust Function
title: rpc_proxy_last_dce_request_call_id
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L522-L538
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback
---

# Signature

`fn rpc_proxy_last_dce_request_call_id(buffer: &[u8]) -> Option<u32>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)

# Called by

- [rpc_proxy_address_book_check_name_fallback](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_address_book_check_name_fallback.md)