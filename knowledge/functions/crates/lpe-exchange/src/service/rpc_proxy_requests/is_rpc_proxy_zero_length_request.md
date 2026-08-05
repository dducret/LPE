---
type: Rust Function
title: is_rpc_proxy_zero_length_request
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L23-L29
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request
---

# Signature

`fn is_rpc_proxy_zero_length_request(headers: &HeaderMap) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [is_rpc_proxy_in_data_channel_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_in_data_channel_request.md)