---
type: Rust Function
title: emsmdb_rpc_request
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1624-L1626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault
---

# Signature

`fn emsmdb_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8>`

# Calls

- [rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request.md)

# Called by

- [rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response.md)
- [rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response.md)
- [rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context.md)
- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault.md)