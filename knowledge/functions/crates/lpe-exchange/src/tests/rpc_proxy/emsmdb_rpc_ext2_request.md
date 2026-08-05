---
type: Rust Function
title: emsmdb_rpc_ext2_request
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1628-L1643
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context
---

# Signature

`fn emsmdb_rpc_ext2_request(call_id: u32, context: &[u8], rop_buffer: &[u8]) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_request.md)

# Called by

- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context.md)