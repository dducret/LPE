---
type: Rust Function
title: abort_submit_audit_entry
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L853-L862
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response
---

# Signature

`pub(super) fn abort_submit_audit_entry( principal: &AccountPrincipal, canonical_message_id: Uuid, ) -> AuditEntryInput`

# Called by

- [append_abort_submit_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)