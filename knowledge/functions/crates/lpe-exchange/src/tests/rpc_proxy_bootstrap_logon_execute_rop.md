---
type: Rust Function
title: rpc_proxy_bootstrap_logon_execute_rop
resource: crates/lpe-exchange/src/tests/mod.rs#L12366-L12374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_wrapped_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context
---

# Signature

`fn rpc_proxy_bootstrap_logon_execute_rop(mailbox: &str) -> Vec<u8>`

# Calls

- [rpc_proxy_wrapped_rop_buffer](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_wrapped_rop_buffer.md)

# Called by

- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context.md)