---
type: Rust Function
title: submitted_message_handle_object
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L108-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/submitted_mapi_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`pub(super) fn submitted_message_handle_object( submitted: &SubmittedMessage, mailboxes: &[JmapMailbox], message_id: u64, ) -> MapiObject`

# Calls

- [submitted_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/submitted_mapi_folder_id.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)