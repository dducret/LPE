---
type: Rust Function
title: nspi_rpc_request
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1616-L1618
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response
---

# Signature

`fn nspi_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8>`

# Calls

- [rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request.md)

# Called by

- [rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses.md)
- [rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response.md)
- [rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response.md)