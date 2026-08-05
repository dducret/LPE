---
type: Rust Function
title: execute_can_skip_identity_scope
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L63-L74
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_has_no_requests
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_targets
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi) fn execute_can_skip_identity_scope( rop_buffer: &[u8], session: &MapiSession, ) -> bool`

# Calls

- [rop_buffer_has_no_requests](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_has_no_requests.md)
- [rop_buffer_is_store_independent_release_only](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only.md)
- [has_notification_targets](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_targets.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)