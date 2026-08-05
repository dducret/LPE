---
type: Rust Function
title: rpc_proxy_endpoint_response_for_fragment
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L636-L731
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_bind_ack
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_context_interface
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response_with_request_auth
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_mgmt_inq_stats_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query
---

# Signature

`fn rpc_proxy_endpoint_response_for_fragment(endpoint_query: &str, bytes: &[u8]) -> Option<Vec<u8>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)
- [rpc_proxy_remember_dce_bind_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts.md)
- [consume_rpc_proxy_out_endpoint_bind_ack](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/consume_rpc_proxy_out_endpoint_bind_ack.md)
- [rpc_proxy_dce_bind_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
- [rpc_proxy_bound_dce_context_interface](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_context_interface.md)
- [rpc_proxy_dce_response_with_request_auth](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response_with_request_auth.md)
- [rpc_proxy_mgmt_inq_stats_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_mgmt_inq_stats_response.md)
- [rpc_proxy_rfri_get_new_dsa_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_new_dsa_response.md)
- [rpc_proxy_rfri_get_fqdn_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rfri_get_fqdn_response.md)
- [rpc_proxy_emsmdb_disconnect_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_disconnect_response.md)
- [rpc_proxy_emsmdb_connect_ex_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response.md)
- [rpc_proxy_emsmdb_rpc_ext2_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response.md)
- [rpc_proxy_nspi_response_for_opnum](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum.md)

# Called by

- [rpc_proxy_in_channel_response_for_endpoint_query](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query.md)