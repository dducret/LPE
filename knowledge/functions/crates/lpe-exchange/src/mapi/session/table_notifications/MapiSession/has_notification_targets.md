---
type: Rust Method
title: has_notification_targets
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L101-L109
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope
---

# Signature

`pub(in crate::mapi) fn has_notification_targets(&self) -> bool`

# Called by

- [execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [execute_can_skip_identity_scope](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope.md)