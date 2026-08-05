---
type: Rust Function
title: rpc_context_session_id
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L281-L287
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
---

# Signature

`pub(in crate::mapi) fn rpc_context_session_id(context_handle: &[u8]) -> Option<String>`

# Called by

- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)