---
type: Rust Function
title: current_windows_filetime
resource: crates/lpe-domain/src/civil_time.rs#L75-L88
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/properties/task/current_task_completion_time
---

# Signature

`pub fn current_windows_filetime() -> u64`

# Called by

- [append_create_attachment_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_create_message_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [current_task_completion_time](../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/current_task_completion_time.md)