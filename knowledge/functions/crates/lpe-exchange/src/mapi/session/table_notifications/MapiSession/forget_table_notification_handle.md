---
type: Rust Method
title: forget_table_notification_handle
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L67-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table
  - functions/crates/lpe-exchange/src/mapi/session/release_handle_slot
---

# Signature

`pub(in crate::mapi) fn forget_table_notification_handle(&mut self, handle: u32)`

# Calls

- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [execute_preserves_pending_table_notification_after_releasing_its_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table.md)
- [release_handle_slot](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/release_handle_slot.md)