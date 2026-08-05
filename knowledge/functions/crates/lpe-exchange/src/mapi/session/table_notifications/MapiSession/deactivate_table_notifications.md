---
type: Rust Method
title: deactivate_table_notifications
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L61-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
---

# Signature

`pub(in crate::mapi) fn deactivate_table_notifications(&mut self, handle: Option<u32>)`

# Calls

- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_execute_status_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)