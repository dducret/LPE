---
type: Rust Function
title: map_update_message_flag_request
resource: crates/lpe-admin-api/src/workspace.rs#L437-L501
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store
  - functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_complete_and_clear_states
  - functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_due_date_controls
  - functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_reminder_controls
---

# Signature

`fn map_update_message_flag_request(request: &UpdateMessageFlagRequest) -> JmapEmailFollowupUpdate`

# Called by

- [update_message_flag_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store.md)
- [update_message_flag_request_maps_complete_and_clear_states](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_complete_and_clear_states.md)
- [update_message_flag_request_maps_due_date_controls](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_due_date_controls.md)
- [update_message_flag_request_maps_reminder_controls](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_reminder_controls.md)