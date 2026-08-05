---
type: Rust Function
title: open_message_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L40-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(super) fn open_message_folder_id(request: &RopRequest, message_id: u64) -> u64`

# Calls

- [is_outlook_local_freebusy_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)