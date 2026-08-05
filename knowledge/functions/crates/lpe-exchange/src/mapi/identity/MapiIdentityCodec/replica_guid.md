---
type: Rust Method
title: replica_guid
resource: crates/lpe-exchange/src/mapi/identity.rs#L263-L265
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context
---

# Signature

`pub(crate) fn replica_guid(&self) -> [u8; 16]`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [allocate_logon_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context.md)