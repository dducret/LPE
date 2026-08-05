---
type: Rust Function
title: submit_audit_entry
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L100-L106
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`pub(super) fn submit_audit_entry(principal: &AccountPrincipal, handle: u32) -> AuditEntryInput`

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)