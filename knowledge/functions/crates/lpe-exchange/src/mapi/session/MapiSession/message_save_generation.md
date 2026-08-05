---
type: Rust Method
title: message_save_generation
resource: crates/lpe-exchange/src/mapi/session.rs#L131-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_handle_generation
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_saved
---

# Signature

`pub(in crate::mapi) fn message_save_generation(&self, folder_id: u64, message_id: u64) -> u64`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [record_message_handle_generation](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_handle_generation.md)
- [record_message_saved](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_saved.md)