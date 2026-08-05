---
type: Rust Function
title: rpc_response_context
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1657-L1659
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
---

# Signature

`fn rpc_response_context(response: &[u8]) -> [u8; 20]`

# Called by

- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)