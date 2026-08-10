---
type: Rust Function
title: submitted_mapi_folder_id
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L806-L815
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submitted_message_handle_object
---

# Signature

`pub(in crate::mapi) fn submitted_mapi_folder_id( submitted: &SubmittedMessage, mailboxes: &[JmapMailbox], ) -> u64`

# Called by

- [submitted_message_handle_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submitted_message_handle_object.md)