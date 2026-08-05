---
type: Rust Function
title: rfri_rpc_request
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1620-L1622
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_referral_opnums_get_server_name_responses
---

# Signature

`fn rfri_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8>`

# Calls

- [rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request.md)

# Called by

- [rpc_proxy_in_channel_referral_opnums_get_server_name_responses](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_referral_opnums_get_server_name_responses.md)