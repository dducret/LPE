---
type: Rust Method
title: record_logon_identity
resource: crates/lpe-exchange/src/mapi/session.rs#L46-L48
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context
---

# Signature

`pub(in crate::mapi) fn record_logon_identity(&mut self, identity: MapiLogonIdentityDebug)`

# Called by

- [allocate_logon_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context.md)