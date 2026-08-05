---
type: Rust Function
title: save_flags_are_supported
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L149-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(in crate::mapi::dispatch) fn save_flags_are_supported(request: &RopRequest) -> bool`

# Calls

- [save_disposition](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition.md)

# Called by

- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)